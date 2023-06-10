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

package com.antgroup.geaflow.pdata.graph.view;

import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.graph.base.algo.VertexCentricAlgo;
import com.antgroup.geaflow.api.partition.graph.edge.CustomEdgeVCPartition;
import com.antgroup.geaflow.api.partition.graph.edge.CustomVertexVCPartition;
import com.antgroup.geaflow.api.partition.graph.edge.IGraphVCPartition;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.encoder.EncoderResolver;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import com.antgroup.geaflow.model.graph.message.encoder.GraphMessageEncoders;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.pdata.graph.window.WindowStreamGraph.DefaultEdgePartition;
import com.antgroup.geaflow.pdata.graph.window.WindowStreamGraph.DefaultVertexPartition;
import com.antgroup.geaflow.pdata.stream.Stream;
import com.antgroup.geaflow.pdata.stream.window.WindowDataStream;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.google.common.base.Preconditions;

public abstract class AbstractGraphView<K, VV, EV, M, R> extends WindowDataStream<R> {

    protected long maxIterations;
    protected GraphViewDesc graphViewDesc;
    protected PWindowStream<IVertex<K, VV>> vertexStream;
    protected PWindowStream<IEdge<K, EV>> edgeStream;
    protected GraphExecAlgo graphExecAlgo;
    protected IEncoder<? extends IGraphMessage<K, M>> msgEncoder;

    public AbstractGraphView(IPipelineContext pipelineContext,
                             IViewDesc graphViewDesc,
                             PWindowStream<IVertex<K, VV>> vertexWindowStream,
                             PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext);
        this.graphViewDesc = (GraphViewDesc) graphViewDesc;
        this.vertexStream = vertexWindowStream;
        this.edgeStream = edgeWindowStream;
        super.parallelism = Math.max(vertexStream.getParallelism(), edgeStream.getParallelism());
    }

    protected void processOnVertexCentric(VertexCentricAlgo<K, VV, EV, M> vertexCentricAlgo) {
        this.graphExecAlgo = GraphExecAlgo.VertexCentric;
        this.maxIterations = vertexCentricAlgo.getMaxIterationCount();
        IGraphVCPartition<K> graphPartition = vertexCentricAlgo.getGraphPartition();
        if (graphPartition == null) {
            this.input = (Stream) this.vertexStream.keyBy(new DefaultVertexPartition<>());
            this.edgeStream = this.edgeStream.keyBy(new DefaultEdgePartition<>());
            if (parallelism > graphViewDesc.getShardNum()) {
                this.input.withParallelism(this.graphViewDesc.getShardNum());
                this.edgeStream.withParallelism(this.graphViewDesc.getShardNum());
            }
        } else {
            Preconditions.checkArgument(parallelism <= graphViewDesc.getShardNum(),
                "op parallelism must be <= shard num");
            this.input = (Stream) this.vertexStream.keyBy(new CustomVertexVCPartition<>(graphPartition));
            this.edgeStream  = this.edgeStream.keyBy(new CustomEdgeVCPartition<>(graphPartition));
        }
        IEncoder<K> keyEncoder = vertexCentricAlgo.getKeyEncoder();
        if (keyEncoder == null) {
            keyEncoder = (IEncoder<K>) EncoderResolver.resolveFunction(VertexCentricAlgo.class, vertexCentricAlgo, 0);
        }
        IEncoder<M> msgEncoder = vertexCentricAlgo.getMessageEncoder();
        if (msgEncoder == null) {
            msgEncoder = (IEncoder<M>) EncoderResolver.resolveFunction(VertexCentricAlgo.class, vertexCentricAlgo, 3);
        }
        this.msgEncoder = GraphMessageEncoders.build(keyEncoder, msgEncoder);
    }

    public long getMaxIterations() {
        return maxIterations;
    }

    public PWindowStream<IEdge<K, EV>> getEdges() {
        return this.edgeStream;
    }

    public IEncoder<? extends IGraphMessage<K, M>> getMsgEncoder() {
        return this.msgEncoder;
    }

}
