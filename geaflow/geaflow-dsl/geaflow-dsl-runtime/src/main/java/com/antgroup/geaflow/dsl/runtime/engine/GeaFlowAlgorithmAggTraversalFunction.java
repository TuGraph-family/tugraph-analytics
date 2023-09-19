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

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricAggTraversalFunction;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

public class GeaFlowAlgorithmAggTraversalFunction implements
    VertexCentricAggTraversalFunction<Object, Row, Row, Object, Row, ITraversalAgg, ITraversalAgg> {

    private final AlgorithmUserFunction<Object, Object> userFunction;

    private final Object[] params;

    private GraphSchema graphSchema;

    private VertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext;

    private GeaFlowAlgorithmRuntimeContext algorithmCtx;

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
        this.algorithmCtx = new GeaFlowAlgorithmRuntimeContext(traversalContext, graphSchema);
        this.userFunction.init(algorithmCtx, params);
    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        RowVertex vertex = (RowVertex) traversalContext.vertex().get();
        if (vertex != null) {
            algorithmCtx.setVertexId(vertex.getId());
            userFunction.process(vertex, Collections.emptyIterator());
        }
    }

    @Override
    public void compute(Object vertexId, Iterator<Object> messages) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex vertex = (RowVertex) traversalContext.vertex().get();
        if (vertex != null) {
            Row newValue = algorithmCtx.getVertexNewValue();
            if (newValue != null) {
                vertex.setValue(newValue);
            }
            userFunction.process(vertex, messages);
        }
    }

    @Override
    public void finish() {
        Iterator<Object> idIterator = traversalContext.vertex().loadIdIterator();
        while (idIterator.hasNext()) {
            Object id = idIterator.next();
            algorithmCtx.setVertexId(id);
            RowVertex rowVertex = (RowVertex) traversalContext.vertex().withId(id).get();
            if (rowVertex != null) {
                Row newValue = algorithmCtx.getVertexNewValue();
                if (newValue != null) {
                    rowVertex.setValue(newValue);
                }
                userFunction.finish(rowVertex);
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.algorithmCtx.setAggContext(Objects.requireNonNull(aggContext));
    }
}
