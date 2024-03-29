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

package com.antgroup.geaflow.operator.impl.graph.traversal.dynamic;

import static com.antgroup.geaflow.operator.Constants.GRAPH_VERSION;

import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import com.antgroup.geaflow.common.iterator.CloseableIterator;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.view.graph.GraphViewDesc;

public class DynamicGraphVertexCentricTraversalAllOp<K, VV, EV, M, R,
    FUNC extends IncVertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractDynamicGraphVertexCentricTraversalOp<K, VV, EV, M, R, FUNC> {

    public DynamicGraphVertexCentricTraversalAllOp(
        GraphViewDesc graphViewDesc,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
    }

    @Override
    protected void traversalByRequest() {
        if (!temporaryGraphCache.getAllEvolveVId().isEmpty()) {
            try (CloseableIterator<K> idIterator =
                     graphState.dynamicGraph().V().query(GRAPH_VERSION, keyGroup).idIterator()) {
                while (idIterator.hasNext()) {
                    K vertexId = idIterator.next();
                    ITraversalRequest<K> traversalRequest = new VertexBeginTraversalRequest<>(vertexId);
                    this.graphVCTraversalCtx.init(iterations, vertexId);
                    this.incVcTraversalFunction.init(traversalRequest);
                }
            }
        }
    }
}
