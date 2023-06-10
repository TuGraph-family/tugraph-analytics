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

package com.antgroup.geaflow.operator.impl.graph.traversal.statical;

import com.antgroup.geaflow.api.graph.traversal.VertexCentricTraversal;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphVertexCentricTraversalAllOp<K, VV, EV, M, R> extends
    AbstractStaticGraphVertexCentricTraversalOp<K, VV, EV, M, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        StaticGraphVertexCentricTraversalAllOp.class);

    public StaticGraphVertexCentricTraversalAllOp(GraphViewDesc graphViewDesc,
                                                  VertexCentricTraversal<K, VV, EV, M, R> vcTraversal) {
        super(graphViewDesc, vcTraversal);
    }

    @Override
    protected void traversalByRequest() {
        Iterator<K> idIterator = graphVCTraversalCtx.vertex().loadIdIterator();
        while (idIterator.hasNext()) {
            K vertexId = idIterator.next();
            ITraversalRequest<K> traversalRequest = new VertexBeginTraversalRequest(vertexId);
            this.graphVCTraversalCtx.init(iterations, vertexId);
            this.vcTraversalFunction.init(traversalRequest);
        }
    }

}
