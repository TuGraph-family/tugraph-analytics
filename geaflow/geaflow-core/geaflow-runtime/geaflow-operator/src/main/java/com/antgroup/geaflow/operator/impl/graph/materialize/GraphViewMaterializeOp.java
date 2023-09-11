/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.operator.impl.graph.materialize;

import com.antgroup.geaflow.api.graph.materialize.GraphMaterializeFunction;
import com.antgroup.geaflow.api.trait.CheckpointTrait;
import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphViewMaterializeOp<K, VV, EV> extends AbstractOneInputOperator<Object,
    GraphMaterializeFunction<K, VV, EV>> implements TransactionTrait, CheckpointTrait {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphViewMaterializeOp.class);

    private final GraphViewDesc graphViewDesc;

    protected transient GraphState<K, VV, EV> graphState;

    public GraphViewMaterializeOp(GraphViewDesc graphViewDesc) {
        this.graphViewDesc = graphViewDesc;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        String name = graphViewDesc.getName();
        String storeType = graphViewDesc.getBackend().name();
        GraphStateDescriptor<K, VV, EV> descriptor = GraphStateDescriptor.build(
            graphViewDesc.getName(), storeType);
        descriptor.withDataModel(DataModel.DYNAMIC_GRAPH);
        descriptor.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        descriptor.withMetricGroup(runtimeContext.getMetric());

        int maxPara = graphViewDesc.getShardNum();
        int taskPara = runtimeContext.getTaskArgs().getParallelism();
        Preconditions.checkArgument(taskPara <= maxPara,
            String.format("task parallelism '%s' must be <= shard num(max parallelism) '%s'", taskPara, maxPara));

        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxPara,
            taskPara, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxPara);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);

        int taskId = runtimeContext.getTaskArgs().getTaskId();
        LOGGER.info("opName:{} taskId:{} taskIndex:{} keyGroup:{}", name, taskId,
            taskIndex, keyGroup);
        this.graphState = StateFactory.buildGraphState(descriptor, runtimeContext.getConfiguration());
        recover();
        this.function = new DynamicGraphMaterializeFunction<>(graphState);
    }

    @Override
    protected void process(Object record) throws Exception {
        if (record instanceof IVertex) {
            this.function.materializeVertex((IVertex<K, VV>) record);
        } else {
            this.function.materializeEdge((IEdge<K, EV>) record);
        }
    }

    @Override
    public void checkpoint(long windowId) {
        long checkpointId = graphViewDesc.getCheckpoint(windowId);
        this.graphState.manage().operate().setCheckpointId(checkpointId);
        this.graphState.manage().operate().finish();
        this.graphState.manage().operate().archive();
        LOGGER.info("do checkpoint over, checkpointId: {}", checkpointId);
    }

    @Override
    public void finish(long windowId) {

    }

    @Override
    public void rollback(long batchId) {
        recover(batchId);
    }

    @Override
    public void close() {
        if (this.graphState != null) {
            this.graphState.manage().operate().close();
        }
    }

    protected void recover() {
        recover(this.runtimeContext.getWindowId());
    }

    private void recover(long windowId) {
        long lastCheckPointId;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(),
                this.runtimeContext.getConfiguration());
            lastCheckPointId = keeper.getLatestViewVersion(graphViewDesc.getName());
            LOGGER.info("opName: {} will do recover, ViewMetaBookKeeper version: {}",
                this.opArgs.getOpName(), lastCheckPointId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        if (lastCheckPointId >= 0) {
            LOGGER.info("opName: {} do recover to graph VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        } else {
            //If the graph has a checkpoint, should we recover it
            LOGGER.info("lastCheckPointId < 0, windowId: {}", windowId);
            if (windowId > 1) {
                //Recover checkpoint id for dynamic graph is init graph version add windowId
                long recoverVersionId = graphViewDesc.getCheckpoint(windowId - 1);
                LOGGER.info("opName: {} do recover to latestVersionId: {}", this.opArgs.getOpName(),
                    recoverVersionId);
                graphState.manage().operate().setCheckpointId(recoverVersionId);
                graphState.manage().operate().recover();
            }
        }
    }

    private static class DynamicGraphMaterializeFunction<K, VV, EV> implements GraphMaterializeFunction<K, VV, EV> {

        public static final long VERSION = 0L;

        private final GraphState<K, VV, EV> graphState;

        public DynamicGraphMaterializeFunction(GraphState<K, VV, EV> graphState) {
            this.graphState = graphState;
        }

        @Override
        public void materializeVertex(IVertex<K, VV> vertex) {
            graphState.dynamicGraph().V().add(VERSION, vertex);
        }

        @Override
        public void materializeEdge(IEdge<K, EV> edge) {
            graphState.dynamicGraph().E().add(VERSION, edge);
        }

    }

    @VisibleForTesting
    public GraphState<K, VV, EV> getGraphState() {
        return graphState;
    }
}
