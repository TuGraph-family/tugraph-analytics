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

package com.antgroup.geaflow.operator.impl.graph.compute.dynamic;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;

import com.antgroup.geaflow.api.graph.base.algo.VertexCentricAlgo;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.CheckpointUtil;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.meta.GraphMeta;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.operator.OpArgs;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import com.antgroup.geaflow.operator.impl.graph.compute.dynamic.cache.TemporaryGraphCache;
import com.antgroup.geaflow.state.DataModel;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDynamicGraphVertexCentricOp<K, VV, EV, M, FUNC extends VertexCentricAlgo<K, VV, EV, M>>
    extends AbstractGraphVertexCentricOp<K, VV, EV, M, FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractDynamicGraphVertexCentricOp.class);

    protected TemporaryGraphCache<K, VV, EV> temporaryGraphCache;
    private long checkpointDuration;

    public AbstractDynamicGraphVertexCentricOp(GraphViewDesc graphViewDesc, FUNC func) {
        super(graphViewDesc, func);
        assert !graphViewDesc.isStatic();
        opArgs.setOpType(OpType.INC_VERTEX_CENTRIC_COMPUTE);
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
        this.maxIterations = this.function.getMaxIterationCount();
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.temporaryGraphCache = new TemporaryGraphCache<>();
        this.checkpointDuration = runtimeContext.getConfiguration().getLong(BATCH_NUMBER_PER_CHECKPOINT);
    }

    @Override
    public void addVertex(IVertex<K, VV> vertex) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} iterations:{} add vertex:{}",
                runtimeContext.getTaskArgs().getTaskId(),
                windowId,
                iterations,
                vertex);
        }
        this.temporaryGraphCache.addVertex(vertex);
        this.opInputMeter.mark();
    }

    @Override
    public void addEdge(IEdge<K, EV> edge) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} iterations:{} add edge:{}",
                runtimeContext.getTaskArgs().getTaskId(),
                windowId,
                iterations,
                edge);
        }
        this.temporaryGraphCache.addEdge(edge);
        this.opInputMeter.mark();
    }

    @Override
    public void close() {
        this.temporaryGraphCache.clear();
        this.graphMsgBox.clearInBox();
        this.graphMsgBox.clearOutBox();
        this.graphState.manage().operate().close();
    }

    @Override
    protected GraphStateDescriptor<K, VV, EV> buildGraphStateDesc(String name) {
        GraphStateDescriptor<K, VV, EV> desc =  super.buildGraphStateDesc(name);
        desc.withDataModel(DataModel.DYNAMIC_GRAPH);
        desc.withGraphMeta(new GraphMeta(graphViewDesc.getGraphMetaType()));
        return desc;
    }

    public GraphViewDesc getGraphViewDesc() {
        return graphViewDesc;
    }


    protected void checkpoint() {
        if (CheckpointUtil.needDoCheckpoint(windowId, checkpointDuration)) {
            LOGGER.info("opName:{} do checkpoint for windowId:{}, checkpoint duration:{}",
                this.opArgs.getOpName(), windowId, checkpointDuration);
            long checkpoint = graphViewDesc.getCheckpoint(windowId);
            LOGGER.info("do checkpoint, checkpointId: {}", checkpoint);
            graphState.manage().operate().setCheckpointId(checkpoint);
            graphState.manage().operate().finish();
            graphState.manage().operate().archive();
        }
    }

    @Override
    protected void recover() {
        LOGGER.info("opName: {} will do recover, windowId: {}", this.opArgs.getOpName(),
            this.windowId);
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
            LOGGER.info("opName: {} do recover to state VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        } else {
            LOGGER.info("lastCheckPointId < 0");
            //If the graph has a checkpoint, should we recover it
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
}
