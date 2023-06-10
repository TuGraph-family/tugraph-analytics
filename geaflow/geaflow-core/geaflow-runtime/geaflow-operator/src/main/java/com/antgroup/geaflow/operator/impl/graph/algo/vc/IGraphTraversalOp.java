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

import com.antgroup.geaflow.model.traversal.ITraversalRequest;
import java.util.Iterator;

public interface IGraphTraversalOp<K, VV, EV, M> extends IGraphVertexCentricOp<K, VV, EV, M> {

    /**
     * Add traversal request.
     */
    void addRequest(ITraversalRequest<K> request);

    /**
     * Returns traversal request iterator.
     */
    Iterator<ITraversalRequest<K>> getTraversalRequests();
}
