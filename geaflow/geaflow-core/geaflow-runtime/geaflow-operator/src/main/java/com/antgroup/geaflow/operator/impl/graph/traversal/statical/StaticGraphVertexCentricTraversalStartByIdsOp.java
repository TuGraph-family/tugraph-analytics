/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.operator.impl.graph.traversal.statical;

import com.antgroup.geaflow.api.graph.base.algo.AbstractVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticGraphVertexCentricTraversalStartByIdsOp<K, VV, EV, M, R,
    FUNC extends VertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractStaticGraphVertexCentricTraversalOp<K, VV, EV, M, R, FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        StaticGraphVertexCentricTraversalStartByIdsOp.class);

    private final List<ITraversalRequest<K>> traversalRequests;

    public StaticGraphVertexCentricTraversalStartByIdsOp(
        GraphViewDesc graphViewDesc, VertexBeginTraversalRequest<K> vertexBeginTraversalRequest,
        AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        traversalRequests = new ArrayList<>();
        traversalRequests.add(vertexBeginTraversalRequest);
    }

    public StaticGraphVertexCentricTraversalStartByIdsOp(
        GraphViewDesc graphViewDesc, List<VertexBeginTraversalRequest<K>> vertexBeginTraversalRequests,
        AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        traversalRequests = new ArrayList<>();
        traversalRequests.addAll(vertexBeginTraversalRequests);
    }

    @Override
    public Iterator<ITraversalRequest<K>> getTraversalRequests() {
        List<ITraversalRequest<K>> currentTaskRequest = new ArrayList<>();
        for (ITraversalRequest<K> traversalRequest : traversalRequests) {
            int maxParallelism = graphViewDesc.getShardNum();
            int currentKeyGroup = KeyGroupAssignment.assignToKeyGroup(traversalRequest.getVId(), maxParallelism);
            LOGGER.info("maxParallelism {}", maxParallelism);

            if (currentKeyGroup >= taskKeyGroup.getStartKeyGroup() && currentKeyGroup <= taskKeyGroup.getEndKeyGroup()) {
                currentTaskRequest.add(traversalRequest);
            }
        }
        return currentTaskRequest.iterator();
    }
}
