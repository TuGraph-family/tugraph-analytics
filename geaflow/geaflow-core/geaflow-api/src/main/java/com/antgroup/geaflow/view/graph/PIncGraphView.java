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

package com.antgroup.geaflow.view.graph;

import com.antgroup.geaflow.api.graph.compute.IncVertexCentricAggCompute;
import com.antgroup.geaflow.api.graph.compute.IncVertexCentricCompute;
import com.antgroup.geaflow.api.graph.compute.PGraphCompute;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricAggTraversal;
import com.antgroup.geaflow.api.graph.traversal.IncVertexCentricTraversal;
import com.antgroup.geaflow.api.graph.traversal.PGraphTraversal;

public interface PIncGraphView<K, VV, EV> extends PGraphView<K, VV, EV> {

    /**
     * Incremental graph traversal.
     */
    <M, R> PGraphTraversal<K, R> incrementalTraversal(IncVertexCentricTraversal<K,
        VV, EV, M, R> incVertexCentricTraversal);

    /**
     * Incremental graph traversal with aggregation.
     */
    <M, R, I, PA, PR, GA, GR> PGraphTraversal<K, R> incrementalTraversal(IncVertexCentricAggTraversal<K,
            VV, EV, M, R, I, PA, PR, GA, GR> incVertexCentricTraversal);

    /**
     * Incremental graph compute.
     */
    <M> PGraphCompute<K, VV, EV> incrementalCompute(IncVertexCentricCompute<K, VV, EV, M> incVertexCentricCompute);

    /**
     * Incremental graph compute with aggregation.
     */
    <M, I, PA, PR, GA, GR> PGraphCompute<K, VV, EV> incrementalCompute(
        IncVertexCentricAggCompute<K, VV, EV, M, I, PA, PR, GA, GR> incVertexCentricCompute);

    /**
     * Materialize graph data into graph state.
     */
    void materialize();
}
