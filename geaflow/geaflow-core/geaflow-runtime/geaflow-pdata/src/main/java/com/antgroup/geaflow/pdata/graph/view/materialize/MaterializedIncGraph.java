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

package com.antgroup.geaflow.pdata.graph.view.materialize;

import com.antgroup.geaflow.api.graph.materialize.PGraphMaterialize;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.common.Null;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.graph.materialize.GraphViewMaterializeOp;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.partitioner.impl.KeyPartitioner;
import com.antgroup.geaflow.pdata.graph.view.AbstractGraphView;
import com.antgroup.geaflow.pdata.graph.window.WindowStreamGraph;
import com.antgroup.geaflow.pdata.stream.Stream;
import com.antgroup.geaflow.pdata.stream.TransformType;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import com.antgroup.geaflow.view.IViewDesc;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaterializedIncGraph<K, VV, EV> extends AbstractGraphView<K, VV, EV, Null,
    IVertex<K, VV>> implements PGraphMaterialize<K, VV, EV> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedIncGraph.class);

    public MaterializedIncGraph(IPipelineContext pipelineContext,
                                IViewDesc graphViewDesc,
                                PWindowStream<IVertex<K, VV>> vertexWindowStream,
                                PWindowStream<IEdge<K, EV>> edgeWindowStream) {
        super(pipelineContext, graphViewDesc, vertexWindowStream, edgeWindowStream);
    }

    @Override
    public void materialize() {
        LOGGER.info("call materialize");
        GraphViewMaterializeOp materializeOp = new GraphViewMaterializeOp<>(graphViewDesc);
        super.operator = materializeOp;
        this.opArgs = ((AbstractOperator) operator).getOpArgs();
        this.opArgs.setOpId(getId());
        this.withParallelism(graphViewDesc.getShardNum());
        this.opArgs.setOpName(MaterializedIncGraph.class.getSimpleName());
        assert this.vertexStream.getParallelism() <= graphViewDesc.getShardNum() : "Materialize "
            + "vertexStream parallelism must <= number of graph shard num";
        this.input = (Stream) this.vertexStream
            .keyBy(new WindowStreamGraph.DefaultVertexPartition<>());
        assert this.edgeStream.getParallelism() <= graphViewDesc.getShardNum() : "Materialize "
            + "edgeStream parallelism must <= number of graph shard num";
        this.edgeStream = this.edgeStream
            .keyBy(new WindowStreamGraph.DefaultEdgePartition<>());
        super.context.addPAction(this);

        assert this.getParallelism() == graphViewDesc.getShardNum() : "Materialize parallelism "
            + "must be equal to the graph shard num.";
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.ContinueGraphMaterialize;
    }

    @Override
    public IPartitioner getPartition() {
        return new KeyPartitioner(this.getId());
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withConfig(Map config) {
        this.setConfig(config);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withConfig(String key, String value) {
        this.setConfig(key, value);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withName(String name) {
        this.setName(name);
        return this;
    }

    @Override
    public MaterializedIncGraph<K, VV, EV> withParallelism(int parallelism) {
        this.setParallelism(parallelism);
        return this;
    }
}
