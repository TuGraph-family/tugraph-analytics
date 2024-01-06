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

import com.antgroup.geaflow.api.graph.base.algo.AbstractIncVertexCentricTraversalAlgo;
import com.antgroup.geaflow.api.graph.base.algo.GraphAggregationAlgo;
import com.antgroup.geaflow.api.graph.function.vc.IncVertexCentricAggTraversalFunction;

public abstract class IncVertexCentricAggTraversal<K, VV, EV, M, R, I, PA, PR, GA, GR>
    extends AbstractIncVertexCentricTraversalAlgo<K, VV, EV, M, R,
        IncVertexCentricAggTraversalFunction<K, VV, EV, M, R, I, GR>>
    implements GraphAggregationAlgo<I, PA, PR, GA, GR> {

    public IncVertexCentricAggTraversal(long iterations) {
        super(iterations);
    }

    public IncVertexCentricAggTraversal(long iterations, String name) {
        super(iterations, name);
    }
}
