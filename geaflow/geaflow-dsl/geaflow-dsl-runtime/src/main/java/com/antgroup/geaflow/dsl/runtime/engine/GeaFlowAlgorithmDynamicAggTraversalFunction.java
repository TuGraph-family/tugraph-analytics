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

import com.antgroup.geaflow.api.function.iterator.RichIteratorFunction;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class GeaFlowAlgorithmDynamicAggTraversalFunction
    implements IncVertexCentricAggTraversalFunction<Object, Row, Row, Object, Row, ITraversalAgg,
        ITraversalAgg>, RichIteratorFunction {

    private static final long VERSION = 0L;

    private final AlgorithmUserFunction<Object, Object> userFunction;

    private final Object[] params;

    private GraphSchema graphSchema;

    private IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext;

    private GeaFlowAlgorithmDynamicRuntimeContext algorithmCtx;

    private MutableGraph<Object, Row, Row> mutableGraph;

    public GeaFlowAlgorithmDynamicAggTraversalFunction(GraphSchema graphSchema,
                                                       AlgorithmUserFunction<Object, Object> userFunction,
                                                       Object[] params) {
        this.graphSchema = Objects.requireNonNull(graphSchema);
        this.userFunction = Objects.requireNonNull(userFunction);
        this.params = Objects.requireNonNull(params);
    }

    @Override
    public void open(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> vertexCentricFuncContext) {
        this.traversalContext = vertexCentricFuncContext;
        this.algorithmCtx = new GeaFlowAlgorithmDynamicRuntimeContext(traversalContext, graphSchema);
        this.userFunction.init(algorithmCtx, params);
        this.mutableGraph = traversalContext.getMutableGraph();
    }

    @Override
    public void init(ITraversalRequest<Object> traversalRequest) {
        Object vertexId = traversalRequest.getVId();
        algorithmCtx.setVertexId(vertexId);
        RowVertex vertex = (RowVertex) algorithmCtx.loadVertex();
        if (vertex != null) {
            algorithmCtx.setVertexId(vertex.getId());
            userFunction.process(vertex, Collections.emptyIterator());
        }
    }

    @Override
    public void evolve(Object vertexId, TemporaryGraph<Object, Row, Row> temporaryGraph) {
        IVertex<Object, Row> vertex = temporaryGraph.getVertex();
        if (vertex != null) {
            mutableGraph.addVertex(VERSION, vertex);
        }
        List<IEdge<Object, Row>> edges = temporaryGraph.getEdges();
        if (edges != null) {
            for (IEdge<Object, Row> edge : edges) {
                mutableGraph.addEdge(VERSION, edge);
            }
        }
    }

    @Override
    public void compute(Object vertexId, Iterator<Object> messages) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex vertex = (RowVertex) algorithmCtx.loadVertex();
        if (vertex != null) {
            Row newValue = algorithmCtx.getVertexNewValue();
            if (newValue != null) {
                vertex.setValue(newValue);
            }
            userFunction.process(vertex, messages);
        }
    }

    @Override
    public void finish(Object vertexId, MutableGraph<Object, Row, Row> mutableGraph) {
        algorithmCtx.setVertexId(vertexId);
        RowVertex rowVertex = (RowVertex) algorithmCtx.loadVertex();
        if (rowVertex != null) {
            Row newValue = algorithmCtx.getVertexNewValue();
            if (newValue != null) {
                rowVertex.setValue(newValue);
            }
            userFunction.finish(rowVertex);
        }
    }

    @Override
    public void initIteration(long iterationId) {
    }

    @Override
    public void finishIteration(long iterationId) {
    }

    @Override
    public void initContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.algorithmCtx.setAggContext(Objects.requireNonNull(aggContext));
    }
}
