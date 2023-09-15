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

package com.antgroup.geaflow.api.graph;

import com.antgroup.geaflow.api.graph.compute.PGraphCompute;
import com.antgroup.geaflow.api.graph.compute.VertexCentricAggCompute;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.traversal.PGraphTraversal;
import com.antgroup.geaflow.api.graph.traversal.VertexCentricAggTraversal;
import com.antgroup.geaflow.api.graph.traversal.VertexCentricTraversal;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.graph.edge.IEdge;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

public interface PGraphWindow<K, VV, EV> {

    /**
     * Build ComputeWindowGraph based on vertexCentricCompute function.
     */
    <M> PGraphCompute<K, VV, EV> compute(VertexCentricCompute<K, VV, EV, M> vertexCentricCompute);

    /**
     * Build ComputeWindowGraph based on vertexCentricAggCompute function.
     */
    <M, I, PA, PR, GA, R> PGraphCompute<K, VV ,EV> compute(
        VertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, R> vertexCentricAggCompute);

    /**
     * Build PGraphTraversal based on vertexCentricTraversal function.
     */
    <M, R> PGraphTraversal<K, R> traversal(VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal);

    /**
     * Build PGraphTraversal based on vertexCentricTraversal function.
     */
    <M, R, I, PA, PR, GA, GR> PGraphTraversal<K, R> traversal(
        VertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR> vertexCentricAggTraversal);

    /**
     * Returns the edges.
     */
    PWindowStream<IEdge<K, EV>> getEdges();

    /**
     * Returns the vertices.
     */
    PWindowStream<IVertex<K, VV>> getVertices();
}
