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

package com.antgroup.geaflow.operator.impl.graph.algo.vc;

import com.antgroup.geaflow.api.graph.compute.IncVertexCentricCompute;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import com.antgroup.geaflow.api.graph.traversal.VertexCentricTraversal;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.operator.impl.graph.compute.dynamic.DynamicGraphVertexCentricComputeOp;
import com.antgroup.geaflow.operator.impl.graph.compute.statical.StaticGraphVertexCentricComputeOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalAllOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByIdsOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.dynamic.DynamicGraphVertexCentricTraversalStartByStreamOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalAllOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByIdsOp;
import com.antgroup.geaflow.operator.impl.graph.traversal.statical.StaticGraphVertexCentricTraversalStartByStreamOp;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.List;

public class GraphVertexCentricOpFactory {

    public static <K, VV, EV, M> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricComputeOp(
        GraphViewDesc graphViewDesc, VertexCentricCompute<K, VV, EV, M> vertexCentricCompute) {
        return new StaticGraphVertexCentricComputeOp<>(graphViewDesc, vertexCentricCompute);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalStartByStreamOp<>(graphViewDesc,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal) {
        return new StaticGraphVertexCentricTraversalAllOp<>(graphViewDesc, vertexCentricTraversal);
    }


    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc, VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new StaticGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequest,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildStaticGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        VertexCentricTraversal<K, VV, EV, M, R> vertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new StaticGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequests,
            vertexCentricTraversal);
    }

    public static <K, VV, EV, M> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricComputeOp(
        GraphViewDesc graphViewDesc, IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute) {
        return new DynamicGraphVertexCentricComputeOp(graphViewDesc, incVertexCentricCompute);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal,
        VertexBeginTraversalRequest<K> traversalRequest) {
        return new DynamicGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc, traversalRequest,
            incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalAllOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal) {
        return new DynamicGraphVertexCentricTraversalStartByStreamOp<>(graphViewDesc, incVertexCentricTraversal);
    }

    public static <K, VV, EV, M, R> IGraphVertexCentricOp<K, VV, EV, M> buildDynamicGraphVertexCentricTraversalOp(
        GraphViewDesc graphViewDesc,
        IncVertexCentricTraversal<K, VV, EV, M, R> incVertexCentricTraversal,
        List<VertexBeginTraversalRequest<K>> traversalRequests) {
        return new DynamicGraphVertexCentricTraversalStartByIdsOp<>(graphViewDesc,
            traversalRequests,
            incVertexCentricTraversal);
    }

}
