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

package com.antgroup.geaflow.api.graph.traversal;

import com.antgroup.geaflow.api.graph.base.algo.AbstractVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;

public abstract class VertexCentricTraversal<K, VV, EV, M, R>
    extends AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R, VertexCentricTraversalFunction<K, VV, EV, M, R>> {

    public VertexCentricTraversal(long iterations) {
        super(iterations);
    }

    public VertexCentricTraversal(long iterations, String name) {
        super(iterations, name);
    }
}
