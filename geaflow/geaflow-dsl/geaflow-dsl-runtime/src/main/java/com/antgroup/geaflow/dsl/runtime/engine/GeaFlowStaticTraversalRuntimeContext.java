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
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.VertexCentricTraversalFuncContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.traversal.message.KVTraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeaFlowStaticTraversalRuntimeContext extends AbstractTraversalRuntimeContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeaFlowStaticTraversalRuntimeContext.class);

    private final VertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> traversalContext;

    public GeaFlowStaticTraversalRuntimeContext(
        VertexCentricTraversalFuncContext<Object, Row, Row, MessageBox, ITreePath> traversalContext) {
        super(traversalContext.vertex(), traversalContext.edges());
        this.traversalContext = traversalContext;
    }

    @Override
    public Configuration getConfig() {
        return traversalContext.getRuntimeContext().getConfiguration();
    }

    @Override
    public long getIterationId() {
        return traversalContext.getIterationId();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void sendBroadcastMessage(Object vertexId, MessageBox messageBox) {
        traversalContext.broadcast(new DefaultGraphMessage<>(vertexId, messageBox));
    }

    @Override
    protected void sendMessage(Object vertexId, MessageBox messageBox) {
        traversalContext.sendMessage(vertexId, messageBox);
    }

    @Override
    public void takePath(ITreePath treePath) {
        traversalContext.takeResponse(new TraversalResponse(treePath));
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
        return traversalContext.getRuntimeContext();
    }
}
