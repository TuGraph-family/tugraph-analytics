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

package com.antgroup.geaflow.operator.impl.graph.algo.vc;

import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.base.algo.VertexCentricAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.partition.graph.edge.IGraphVCPartition;
import com.antgroup.geaflow.collector.AbstractCollector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.context.AbstractRuntimeContext;
import com.antgroup.geaflow.metrics.common.MetricNameFormatter;
import com.antgroup.geaflow.metrics.common.api.Meter;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.operator.OpArgs;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.GraphMsgBoxFactory;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox.IGraphMsgBox;
import com.antgroup.geaflow.operator.impl.iterator.IteratorOperator;
import com.antgroup.geaflow.state.GraphState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.descriptor.GraphStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;
import com.antgroup.geaflow.view.IViewDesc.BackendType;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.meta.ViewMetaBookKeeper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGraphVertexCentricOp<K, VV, EV, M,
    FUNC extends VertexCentricAlgo<K, VV, EV, M>> extends
    AbstractOperator<FUNC> implements IGraphVertexCentricOp<K, VV, EV, M>, IteratorOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        AbstractGraphVertexCentricOp.class);

    protected int taskId;

    protected VertexCentricCombineFunction<M> msgCombineFunction;
    protected IGraphVCPartition<K> graphVCPartition;
    protected final GraphViewDesc graphViewDesc;

    protected long maxIterations;
    protected long iterations;
    protected long windowId;

    protected KeyGroup keyGroup;
    protected GraphState<K, VV, EV> graphState;
    protected IGraphMsgBox<K, M> graphMsgBox;

    protected Map<String, ICollector> collectorMap;
    protected ICollector<IGraphMessage<K, M>> messageCollector;
    protected Meter msgMeter;

    public AbstractGraphVertexCentricOp(GraphViewDesc graphViewDesc, FUNC func) {
        super(func);
        this.graphViewDesc = graphViewDesc;
        this.maxIterations = func.getMaxIterationCount();
        opArgs.setChainStrategy(OpArgs.ChainStrategy.NEVER);
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);

        this.msgCombineFunction = function.getCombineFunction();
        this.graphVCPartition = function.getGraphPartition();
        this.windowId = runtimeContext.getWindowId();
        this.msgMeter = this.metricGroup.meter(
            MetricNameFormatter.iterationMsgMetricName(this.getClass(), this.opArgs.getOpId()));

        GraphStateDescriptor<K, VV, EV> desc = buildGraphStateDesc(opArgs.getOpName());
        desc.withMetricGroup(runtimeContext.getMetric());
        this.taskId = runtimeContext.getTaskArgs().getTaskId();
        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        LOGGER.info("opName:{} taskId:{} taskIndex:{} keyGroup:{}", this.opArgs.getOpName(),
            taskId,
            taskIndex,
            desc.getKeyGroup());

        this.graphState = StateFactory.buildGraphState(desc, runtimeContext.getConfiguration());
        recover();

        collectorMap = new HashMap<>();
        for (ICollector collector : this.collectors) {
            collectorMap.put(collector.getTag(), collector);
        }
        this.messageCollector = collectorMap.get(GraphRecordNames.Message.name());
        if (this.messageCollector instanceof AbstractCollector) {
            ((AbstractCollector) this.messageCollector).setOutputMetric(this.msgMeter);
        }
        this.graphMsgBox = GraphMsgBoxFactory.buildMessageBox(this.messageCollector, this.msgCombineFunction);
    }

    protected GraphStateDescriptor<K, VV, EV> buildGraphStateDesc(String name) {
        int taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        int taskPara = runtimeContext.getTaskArgs().getParallelism();
        BackendType backendType = graphViewDesc.getBackend();
        GraphStateDescriptor<K, VV, EV> desc = GraphStateDescriptor.build(graphViewDesc.getName()
            , backendType.name());

        int maxPara = graphViewDesc.getShardNum();
        Preconditions.checkArgument(taskPara <= maxPara,
            String.format("task parallelism '%s' must be <= shard num(max parallelism) '%s'",
                taskPara, maxPara));

        keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(maxPara, taskPara, taskIndex);
        IKeyGroupAssigner keyGroupAssigner =
            KeyGroupAssignerFactory.createKeyGroupAssigner(keyGroup, taskIndex, maxPara);
        desc.withKeyGroup(keyGroup);
        desc.withKeyGroupAssigner(keyGroupAssigner);
        return desc;
    }

    @Override
    public void processMessage(IGraphMessage<K, M> graphMessage) {
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} Iteration:{} add message:{}", taskId, windowId,
                iterations,
                graphMessage);
        }
        K vertexId = graphMessage.getTargetVId();
        while (graphMessage.hasNext()) {
            this.graphMsgBox.addInMessages(vertexId, graphMessage.next());
        }
        this.opInputMeter.mark();
    }

    @Override
    public void initIteration(long iterations) {
        this.iterations = iterations;
        this.windowId = opContext.getRuntimeContext().getWindowId();
        ((AbstractRuntimeContext) this.runtimeContext).updateWindowId(windowId);
        if (enableDebug) {
            LOGGER.info("taskId:{} windowId:{} init Iteration:{}", taskId, windowId, iterations);
        }
        this.iterations = iterations;
        if (function instanceof RichIteratorFunction) {
            ((RichIteratorFunction) function).initIteration(iterations);
        }
    }

    @Override
    public long getMaxIterationCount() {
        return this.maxIterations;
    }

    @Override
    public void finishIteration(long iteration) {
        this.ticToc.tic();
        this.doFinishIteration(iteration);
        this.metricGroup.histogram(
            MetricNameFormatter.iterationFinishMetricName(this.getClass(), this.opArgs.getOpId(), iteration)
        ).update(this.ticToc.toc());
    }

    public abstract void doFinishIteration(long iteration);

    @Override
    public void close() {
        this.graphMsgBox.clearInBox();
        this.graphMsgBox.clearOutBox();
        this.graphState.manage().operate().close();
    }

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
        }
    }

}
