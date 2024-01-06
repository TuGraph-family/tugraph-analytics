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

package com.antgroup.geaflow.pdata.graph.view.traversal;

import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import com.antgroup.geaflow.api.graph.traversal.PGraphTraversal;
import com.antgroup.geaflow.api.partition.graph.request.DefaultTraversalRequestPartition;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowBroadcastStream;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.GraphVertexCentricOpFactory;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import com.antgroup.geaflow.pdata.graph.view.AbstractGraphView;
import com.antgroup.geaflow.pdata.stream.TransformType;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;

public class TraversalIncGraph<K, VV, EV, M, R> extends AbstractGraphView<K, VV, EV, M,
    ITraversalResponse<R>> implements PGraphTraversal<K, R> {

    protected PWindowStream<? extends ITraversalRequest<K>> requestStream;
    protected AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, ?> incVertexCentricTraversal;

    public TraversalIncGraph(IPipelineContext pipelineContext,
                             IViewDesc graphViewDesc,
                             PWindowStream<IVertex<K, VV>> vertexWindowStream,
                             PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, graphViewDesc, vertexWindowStream, edgeWindowStream);
        this.vertexStream = vertexWindowStream;
        this.edgeStream = edgeWindowStream;
        this.graphViewDesc = (GraphViewDesc) graphViewDesc;
        super.parallelism = Math.max(vertexStream.getParallelism(), edgeStream.getParallelism());
    }

    public TraversalIncGraph<K, VV, EV, M, R> traversalOnVertexCentric(
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, ?> incVertexCentricTraversal) {
        processOnVertexCentric(incVertexCentricTraversal);
        this.incVertexCentricTraversal = incVertexCentricTraversal;
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start() {
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalAllOp(graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalAllOp(graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(K vId) {
        return start(Lists.newArrayList(vId));
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(List<K> vIds) {
        List<VertexBeginTraversalRequest<K>> vertexBeginTraversalRequests = new ArrayList<>();
        for (K vId : vIds) {
            VertexBeginTraversalRequest<K> vertexBeginTraversalRequest =
                new VertexBeginTraversalRequest<>(
                vId);
            vertexBeginTraversalRequests.add(vertexBeginTraversalRequest);
        }

        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal,
                vertexBeginTraversalRequests);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal,
                vertexBeginTraversalRequests);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public PWindowStream<ITraversalResponse<R>> start(
        PWindowStream<? extends ITraversalRequest<K>> requests) {
        this.requestStream = requests instanceof PWindowBroadcastStream
                             ? requests : requests.keyBy(new DefaultTraversalRequestPartition());
        IGraphVertexCentricOp<K, VV, EV, M> traversalOp;
        if (incVertexCentricTraversal instanceof GraphAggregationAlgo) {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricAggTraversal<K, VV, EV, M, R, ?, ?, ?, ?, ?>) incVertexCentricTraversal);
        } else {
            traversalOp = GraphVertexCentricOpFactory.buildDynamicGraphVertexCentricTraversalOp(
                graphViewDesc,
                (IncVertexCentricTraversal<K, VV, EV, M, R>) incVertexCentricTraversal);
        }
        super.operator = (Operator) traversalOp;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(incVertexCentricTraversal.getName());
        this.opArgs.setParallelism(this.parallelism);
        return this;
    }

    @Override
    public GraphExecAlgo getGraphTraversalType() {
        return graphExecAlgo;
    }

    @Override
    public TraversalIncGraph<K, VV, EV, M, R> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    public PWindowStream<? extends ITraversalRequest<K>> getRequestStream() {
        return requestStream;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueGraphTraversal;
    }
}
