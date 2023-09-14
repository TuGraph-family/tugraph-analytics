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

import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricTraversalFunction;
import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import com.antgroup.geaflow.model.traversal.impl.VertexBeginTraversalRequest;
import com.antgroup.geaflow.utils.keygroup.KeyGroupAssignment;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicGraphVertexCentricTraversalStartByIdsOp<K, VV, EV, M, R,
    FUNC extends IncVertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends AbstractDynamicGraphVertexCentricTraversalOp<K, VV, EV, M, R, FUNC> {

    private static final Logger LOGGER = LoggerFactory.getLogger(
        DynamicGraphVertexCentricTraversalStartByIdsOp.class);

    private final List<ITraversalRequest<K>> traversalRequests;

    public DynamicGraphVertexCentricTraversalStartByIdsOp(
        GraphViewDesc graphViewDesc,
        VertexBeginTraversalRequest<K> vertexBeginTraversalRequest,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        traversalRequests = new ArrayList<>();
        traversalRequests.add(vertexBeginTraversalRequest);
    }

    public DynamicGraphVertexCentricTraversalStartByIdsOp(
        GraphViewDesc graphViewDesc,
        List<VertexBeginTraversalRequest<K>> vertexBeginTraversalRequests,
        AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R, FUNC> vcTraversal) {
        super(graphViewDesc, vcTraversal);
        traversalRequests = new ArrayList<>();
        traversalRequests.addAll(vertexBeginTraversalRequests);
    }

    @Override
    protected void traversalByRequest() {
        if (!temporaryGraphCache.getAllEvolveVId().isEmpty()) {
            if (enableDebug) {
                LOGGER.info("taskId:{} windowId:{} iterations:{} is not empty",
                    runtimeContext.getTaskArgs().getTaskId(), windowId, iterations);
            }
            super.traversalByRequest();
        } else {
            LOGGER.info("taskId:{} windowId:{} iterations:{} is empty",
                runtimeContext.getTaskArgs().getTaskId(), windowId,
                iterations);
        }
    }

    @Override
    public Iterator<ITraversalRequest<K>> getTraversalRequests() {
        List<ITraversalRequest<K>> currentTaskRequest = new ArrayList<>();
        for (ITraversalRequest<K> traversalRequest : traversalRequests) {
            int maxParallelism = graphViewDesc.getShardNum();
            int currentKeyGroup = KeyGroupAssignment.assignToKeyGroup(traversalRequest.getVId(),
                maxParallelism);
            if (currentKeyGroup >= keyGroup.getStartKeyGroup() && currentKeyGroup <= keyGroup.getEndKeyGroup()) {
                currentTaskRequest.add(traversalRequest);
            }
        }
        return currentTaskRequest.iterator();
    }
}
