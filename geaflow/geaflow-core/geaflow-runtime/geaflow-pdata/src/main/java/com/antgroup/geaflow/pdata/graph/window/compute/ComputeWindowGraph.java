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

package com.antgroup.geaflow.pdata.graph.window.compute;

import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.graph.compute.PGraphCompute;
import com.antgroup.geaflow.api.graph.compute.VertexCentricAggCompute;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.GraphVertexCentricOpFactory;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricAggOp;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricOp;
import com.antgroup.geaflow.pdata.graph.window.AbstractGraphWindow;
import com.antgroup.geaflow.pdata.stream.TransformType;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import com.antgroup.geaflow.view.graph.GraphViewDesc;

public class ComputeWindowGraph<K, VV, EV, M> extends AbstractGraphWindow<K, VV,
    EV, M, IVertex<K, VV>> implements PGraphCompute<K, VV, EV> {

    public ComputeWindowGraph(IPipelineContext pipelineContext,
                              PWindowStream<IVertex<K, VV>> vertexStream,
                              PWindowStream<IEdge<K, EV>> edgeStream) {
        super(pipelineContext, vertexStream, edgeStream);
    }

    public PWindowStream<IVertex<K, VV>> computeOnVertexCentric(GraphViewDesc graphViewDesc,
                                                                VertexCentricCompute<K, VV, EV, M> vertexCentricCompute) {
        processOnVertexCentric(vertexCentricCompute);

        IGraphVertexCentricOp<K, VV, EV, M> graphVertexCentricComputeOp =
            GraphVertexCentricOpFactory.buildStaticGraphVertexCentricComputeOp(graphViewDesc,
                vertexCentricCompute);

        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricCompute.getName());
        return this;
    }

    public <I, PA, PR, GA, R> PWindowStream<IVertex<K, VV>> computeOnVertexCentric(
        GraphViewDesc graphViewDesc,
        VertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, R> vertexCentricAggCompute) {
        processOnVertexCentric(vertexCentricAggCompute);

        IGraphVertexCentricAggOp<K, VV, EV, M, I, PA, PR, R> graphVertexCentricComputeOp =
            GraphVertexCentricOpFactory.buildStaticGraphVertexCentricAggComputeOp(graphViewDesc,
                vertexCentricAggCompute);
        super.operator = (Operator) graphVertexCentricComputeOp;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.opArgs.setOpName(vertexCentricAggCompute.getName());
        return this;
    }


    @Override
    public TransformType getTransformType() {
        return TransformType.WindowGraphCompute;
    }

    @Override
    public PWindowStream<IVertex<K, VV>> getVertices() {
        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute() {
        return this;
    }

    @Override
    public PGraphCompute<K, VV, EV> compute(int parallelism) {
        super.parallelism = parallelism;
        return this;
    }

    @Override
    public GraphExecAlgo getGraphComputeType() {
        return GraphExecAlgo.VertexCentric;
    }

}
