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

import com.antgroup.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction.VertexCentricAggContext;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.IncVertexCentricTraversalFuncContext;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction.TraversalGraphSnapShot;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalEdgeQuery;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction.TraversalVertexQuery;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ITraversalAgg;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.model.traversal.TraversalType.ResponseType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GeaFlowAlgorithmDynamicRuntimeContext implements AlgorithmRuntimeContext<Object, Object> {

    private final IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> incVCTraversalCtx;

    protected VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext;

    private final GraphSchema graphSchema;

    protected TraversalVertexQuery<Object, Row> vertexQuery;

    protected TraversalEdgeQuery<Object, Row> edgeQuery;

    private Object vertexId;

    private long iterationId = -1L;

    private final Map<Object, Row> vertexId2NewValue = new HashMap<>();

    public GeaFlowAlgorithmDynamicRuntimeContext(
        IncVertexCentricTraversalFuncContext<Object, Row, Row, Object, Row> traversalContext,
        GraphSchema graphSchema) {
        this.incVCTraversalCtx = traversalContext;
        this.graphSchema = graphSchema;
        TraversalGraphSnapShot<Object, Row, Row> graphSnapShot = incVCTraversalCtx.getHistoricalGraph()
            .getSnapShot(0L);
        this.vertexQuery = graphSnapShot.vertex();
        this.edgeQuery = graphSnapShot.edges();
    }

    public void setVertexId(Object vertexId) {
        this.vertexId = vertexId;
        this.vertexQuery.withId(vertexId);
        this.edgeQuery.withId(vertexId);
    }

    public IVertex loadVertex() {
        return vertexQuery.get();
    }

    public Iterator<Object> loadAllVertex() {
        return vertexQuery.loadIdIterator();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RowEdge> loadEdges(EdgeDirection direction) {
        switch (direction) {
            case OUT:
                return (List) edgeQuery.getOutEdges();
            case IN:
                return (List) edgeQuery.getInEdges();
            case BOTH:
                List<RowEdge> edges = new ArrayList<>();
                edges.addAll((List) edgeQuery.getOutEdges());
                edges.addAll((List) edgeQuery.getInEdges());
                return edges;
            default:
                throw new GeaFlowDSLException("Illegal edge direction: " + direction);
        }
    }

    @Override
    public void sendMessage(Object vertexId, Object message) {
        incVCTraversalCtx.sendMessage(vertexId, message);
        if (getCurrentIterationId() > iterationId) {
            iterationId = getCurrentIterationId();
            aggContext.aggregate(GeaFlowKVAlgorithmAggregateFunction.getAlgorithmAgg(iterationId));
        }
    }

    @Override
    public void updateVertexValue(Row value) {
        vertexId2NewValue.put(vertexId, value);
    }

    @Override
    public void take(Row row) {
        incVCTraversalCtx.takeResponse(new AlgorithmResponse(row));
    }

    public Row getVertexNewValue() {
        return vertexId2NewValue.get(vertexId);
    }

    public long getCurrentIterationId() {
        return incVCTraversalCtx.getIterationId();
    }

    @Override
    public GraphSchema getGraphSchema() {
        return graphSchema;
    }

    public VertexCentricAggContext<ITraversalAgg, ITraversalAgg> getAggContext() {
        return aggContext;
    }

    public void setAggContext(VertexCentricAggContext<ITraversalAgg, ITraversalAgg> aggContext) {
        this.aggContext = Objects.requireNonNull(aggContext);
    }

    private static class AlgorithmResponse implements ITraversalResponse<Row> {

        private final Row row;

        public AlgorithmResponse(Row row) {
            this.row = row;
        }

        @Override
        public long getResponseId() {
            return 0;
        }

        @Override
        public Row getResponse() {
            return row;
        }

        @Override
        public ResponseType getType() {
            return ResponseType.Vertex;
        }
    }
}
