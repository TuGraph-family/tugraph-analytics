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

package com.antgroup.geaflow.dsl.runtime.traversal;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction.VertexCentricAggContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.CallRequestId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EdgeGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.message.IMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.state.pushdown.filter.IFilter;
import java.util.Iterator;

public interface TraversalRuntimeContext {

    Configuration getConfig();

    DagTopologyGroup getTopology();

    void setTopology(DagTopologyGroup topology);

    long getIterationId();

    RowVertex getVertex();

    void setVertex(RowVertex vertex);

    Object getRequestId();

    void setCurrentOpId(long operatorId);

    long getCurrentOpId();

    void setRequest(ParameterRequest parameterRequest);

    ParameterRequest getRequest();

    default Row getParameters() {
        if (getRequest() != null) {
            return getRequest().getParameters();
        }
        return null;
    }

    void setMessageBox(MessageBox messageBox);

    <M extends IMessage> M getMessage(MessageType messageType);

    EdgeGroup loadEdges(IFilter loadEdgesFilter);

    RowVertex loadVertex(Object vertexId, IFilter loadVertexFilter, GraphSchema graphSchema, IType<?>[] addingVertexFieldTypes);

    Iterator<Object> loadAllVertex();

    void sendMessage(Object vertexId, IMessage message, long receiverId, long... otherReceiveIds);

    void broadcast(IMessage message, long receiverId, long... otherReceiveIds);

    void takePath(ITreePath treePath);

    void sendCoordinator(String name, Object value);

    VertexCentricAggContext<ITraversalAgg, ITraversalAgg> getAggContext();

    void setAggContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext);

    RuntimeContext getRuntimeContext();

    MetricGroup getMetric();

    int getNumTasks();

    int getTaskIndex();

    void push(long opId, CallContext callContext);

    void pop(long opId);

    void stashCallRequestId(CallRequestId requestId);

    Iterable<CallRequestId> takeCallRequestIds();

    void setInputOperatorId(long id);

    long getInputOperatorId();

    void addFieldToVertex(Object vertexId, int index, Object value);

    long createUniqueId(long idInTask);
}
