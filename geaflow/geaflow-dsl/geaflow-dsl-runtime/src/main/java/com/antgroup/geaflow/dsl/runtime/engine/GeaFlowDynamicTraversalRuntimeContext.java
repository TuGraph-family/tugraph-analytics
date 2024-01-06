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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.traversal.message.KVTraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowDynamicTraversalRuntimeContext extends AbstractTraversalRuntimeContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowDynamicTraversalRuntimeContext.class);

    private final IncVertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> incVCTraversalCtx;

    public GeaFlowDynamicTraversalRuntimeContext(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> incVCTraversalCtx) {
        this.incVCTraversalCtx = incVCTraversalCtx;
        TraversalGraphSnapShot<Object, Row, Row> graphSnapShot = incVCTraversalCtx.getHistoricalGraph()
            .getSnapShot(0L);
        this.vertexQuery = graphSnapShot.vertex();
        this.edgeQuery = graphSnapShot.edges();
    }

    @Override
    public Configuration getConfig() {
        return incVCTraversalCtx.getRuntimeContext().getConfiguration();
    }

    @Override
    public long getIterationId() {
        return incVCTraversalCtx.getIterationId();
    }

    @Override
    protected void sendBroadcastMessage(Object vertexId, MessageBox messageBox) {
        incVCTraversalCtx.broadcast(new DefaultGraphMessage<>(vertexId, messageBox));
    }

    @Override
    protected void sendMessage(Object vertexId, MessageBox messageBox) {
        incVCTraversalCtx.sendMessage(vertexId, messageBox);
    }

    @Override
    public void takePath(ITreePath treePath) {
        incVCTraversalCtx.takeResponse(new TraversalResponse(treePath));
    }

    @Override
    public void sendCoordinator(String name, Object value) {
        LOGGER.info("task: {} send to coordinator {}:{} isAggTraversal:{}", getTaskIndex(), name,
            value, aggContext != null);
        if (aggContext != null) {
            aggContext.aggregate(new KVTraversalAgg(name, value));
        }
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return incVCTraversalCtx.getRuntimeContext();
    }
}
