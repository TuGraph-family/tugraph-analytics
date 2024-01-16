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
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
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
import com.antgroup.geaflow.state.graph.StateMode;
import com.antgroup.geaflow.state.manage.LoadOption;
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
    protected KeyGroup taskKeyGroup;
    protected GraphState<K, VV, EV> graphState;
    protected IGraphMsgBox<K, M> graphMsgBox;
    protected boolean shareEnable;

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

        shareEnable = runtimeContext.getConfiguration().getBoolean(FrameworkConfigKeys.SERVICE_SHARE_ENABLE);

        GraphStateDescriptor<K, VV, EV> desc = buildGraphStateDesc(opArgs.getOpName());
        desc.withMetricGroup(runtimeContext.getMetric());
        this.graphState = StateFactory.buildGraphState(desc, runtimeContext.getConfiguration());
        LOGGER.info("ThreadId {}, open graphState", Thread.currentThread().getId());
        if (!shareEnable) {
            this.taskKeyGroup = keyGroup;
            LOGGER.info("recovery graph state {}", graphState);
            recover();
        } else {
            load();
            LOGGER.info("processIndex {} taskIndex {} load shard {}, load graph state {}",
                runtimeContext.getTaskArgs().getProcessIndex(), runtimeContext.getTaskArgs().getTaskIndex(), keyGroup, graphState);
        }

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
        this.taskId = runtimeContext.getTaskArgs().getTaskId();

        int containerNum = runtimeContext.getConfiguration().getInteger(ExecutionConfigKeys.CONTAINER_NUM);
        int processIndex = runtimeContext.getTaskArgs().getProcessIndex();
        int taskIndex = shareEnable ? processIndex : runtimeContext.getTaskArgs().getTaskIndex();
        int taskPara = shareEnable ? containerNum : runtimeContext.getTaskArgs().getParallelism();
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
        if (shareEnable) {
            LOGGER.info("enable state singleton");
            desc.withSingleton();
            desc.withStateMode(StateMode.RDONLY);
        }
        LOGGER.info("opName:{} taskId:{} taskIndex:{} keyGroup:{} containerNum:{} processIndex: {} real taskIndex:{}", this.opArgs.getOpName(),
            taskId,
            taskIndex,
            desc.getKeyGroup(), containerNum, processIndex, runtimeContext.getTaskArgs().getTaskIndex());
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
        if (!shareEnable) {
            this.graphState.manage().operate().close();
        }
    }

    protected void recover() {
        LOGGER.info("opName: {} will do recover, windowId: {}", this.opArgs.getOpName(), this.windowId);
        long lastCheckPointId = getLatestViewVersion();
        if (lastCheckPointId >= 0) {
            LOGGER.info("opName: {} do recover to state VersionId: {}", this.opArgs.getOpName(),
                lastCheckPointId);
            graphState.manage().operate().setCheckpointId(lastCheckPointId);
            graphState.manage().operate().recover();
        }
    }

    protected void load() {
        LOGGER.info("opName: {} will do load, windowId: {}", this.opArgs.getOpName(), this.windowId);
        long lastCheckPointId = getLatestViewVersion();
        long checkPointId = lastCheckPointId < 0 ? 0 : lastCheckPointId;
        LOGGER.info("opName: {} do load, ViewMetaBookKeeper version: {}, checkPointId {}",
            this.opArgs.getOpName(), lastCheckPointId, checkPointId);

        LoadOption loadOption = LoadOption.of();
        this.taskKeyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            graphViewDesc.getShardNum(),
            runtimeContext.getTaskArgs().getParallelism(),
            runtimeContext.getTaskArgs().getTaskIndex());
        loadOption.withKeyGroup(this.taskKeyGroup);
        loadOption.withCheckpointId(checkPointId);
        graphState.manage().operate().load(loadOption);
        LOGGER.info("opName: {} task key group {} do load successfully", this.opArgs.getOpName(), this.taskKeyGroup);
    }

    private long getLatestViewVersion() {
        long lastCheckPointId;
        try {
            ViewMetaBookKeeper keeper = new ViewMetaBookKeeper(graphViewDesc.getName(),
                this.runtimeContext.getConfiguration());
            lastCheckPointId = keeper.getLatestViewVersion(graphViewDesc.getName());
            LOGGER.info("opName: {} will do recover or load, ViewMetaBookKeeper version: {}",
                this.opArgs.getOpName(), lastCheckPointId);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        return lastCheckPointId;
    }

}
