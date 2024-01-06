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

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggTraversalFunction;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.state.KeyValueState;
import com.antgroup.geaflow.state.StateFactory;
import com.antgroup.geaflow.state.descriptor.KeyValueStateDescriptor;
import com.antgroup.geaflow.utils.keygroup.IKeyGroupAssigner;
import com.antgroup.geaflow.utils.keygroup.KeyGroup;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignerFactory;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class GeaFlowAlgorithmAggTraversalFunction implements
    VertexCentricAggTraversalFunction<Object, Row, Row, Object, Row, ITraversalAgg, ITraversalAgg> {

    private static final String STATE_SUFFIX = "UpdatedValueState";

    private final AlgorithmUserFunction<Object, Object> userFunction;

    private final Object[] params;

    private GraphSchema graphSchema;

    private VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext;

    private GeaFlowAlgorithmRuntimeContext algorithmCtx;

    private transient Set<Object> invokeVIds;

    private transient KeyValueState<Object, Row> vertexUpdateValues;

    public GeaFlowAlgorithmAggTraversalFunction(GraphSchema graphSchema,
                                                AlgorithmUserFunction<Object, Object> userFunction,
                                                Object[] params) {
        this.graphSchema = Objects.requireNonNull(graphSchema);
        this.userFunction = Objects.requireNonNull(userFunction);
        this.params = Objects.requireNonNull(params);
    }

    @Override
    public void open(
        VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> vertexCentricFuncContext) {
        this.traversalContext = vertexCentricFuncContext;
        this.algorithmCtx = new GeaFlowAlgorithmRuntimeContext(this, traversalContext, graphSchema);
        this.userFunction.init(algorithmCtx, params);
        this.invokeVIds = new HashSet<>();
        String stateName = traversalContext.getTraversalOpName() + "_" + STATE_SUFFIX;
        KeyValueStateDescriptor descriptor = KeyValueStateDescriptor.build(
            stateName,
            traversalContext.getRuntimeContext().getConfiguration().getString(SYSTEM_STATE_BACKEND_TYPE));
        int parallelism = traversalContext.getRuntimeContext().getTaskArgs().getParallelism();
        int maxParallelism = traversalContext.getRuntimeContext().getTaskArgs().getMaxParallelism();
        int taskIndex = traversalContext.getRuntimeContext().getTaskArgs().getTaskIndex();
        KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(
            maxParallelism, parallelism, taskIndex);
        descriptor.withKeyGroup(keyGroup);
        IKeyGroupAssigner keyGroupAssigner = KeyGroupAssignerFactory.createKeyGroupAssigner(
            keyGroup, taskIndex, maxParallelism);
        descriptor.withKeyGroupAssigner(keyGroupAssigner);
        long recoverWindowId = traversalContext.getRuntimeContext().getWindowId();
        this.vertexUpdateValues = StateFactory.buildKeyValueState(descriptor,
            traversalContext.getRuntimeContext().getConfiguration());
        if (recoverWindowId > 1) {
            this.vertexUpdateValues.manage().operate().setCheckpointId(recoverWindowId - 1);
            this.vertexUpdateValues.manage().operate().recover();
        }
    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        RowVertex vertex = (RowVertex) traversalContext.vertex().get();
        if (vertex != null) {
            algorithmCtx.setVertexId(vertex.getId());
            addInvokeVertex(vertex);
            Row newValue = getVertexNewValue(vertex.getId());
            userFunction.process(vertex, Optional.ofNullable(newValue), Collections.emptyIterator());
        }
    }

    @Override
    public void compute(Object vertexId, Iterator<Object> messages) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex vertex = (RowVertex) traversalContext.vertex().get();
        if (vertex != null) {
            Row newValue = getVertexNewValue(vertex.getId());
            addInvokeVertex(vertex);
            userFunction.process(vertex, Optional.ofNullable(newValue), messages);
        }
    }

    @Override
    public void finish() {
        Iterator<Object> idIterator = getInvokeVIds();
        while (idIterator.hasNext()) {
            Object id = idIterator.next();
            algorithmCtx.setVertexId(id);
            RowVertex graphVertex = (RowVertex) traversalContext.vertex().withId(id).get();
            if (graphVertex != null) {
                Row newValue = getVertexNewValue(graphVertex.getId());
                userFunction.finish(graphVertex, Optional.ofNullable(newValue));
            }
        }
        algorithmCtx.finish();
        long windowId = traversalContext.getRuntimeContext().getWindowId();
        this.vertexUpdateValues.manage().operate().setCheckpointId(windowId);
        this.vertexUpdateValues.manage().operate().finish();
        this.vertexUpdateValues.manage().operate().archive();
        invokeVIds.clear();
    }

    @Override
    public void close() {
        algorithmCtx.close();
    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.algorithmCtx.setAggContext(Objects.requireNonNull(aggContext));
    }

    public void updateVertexValue(Object vertexId, Row value) {
        vertexUpdateValues.put(vertexId, value);
    }

    public Row getVertexNewValue(Object vertexId) {
        return vertexUpdateValues.get(vertexId);
    }

    public void addInvokeVertex(RowVertex v) {
        invokeVIds.add(v.getId());
    }

    public Iterator<Object> getInvokeVIds() {
        return invokeVIds.iterator();
    }

}
