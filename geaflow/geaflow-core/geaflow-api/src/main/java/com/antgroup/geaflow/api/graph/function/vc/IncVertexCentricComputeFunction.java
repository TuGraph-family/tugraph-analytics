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

package com.antgroup.geaflow.api.graph.function.vc;

import com.antgroup.geaflow.api.graph.function.vc.base.IncVertexCentricFunction;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

/**
 * Interface for incremental vertex centric compute function with graph aggregation.
 * @param <K> The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M> The message type during iterations.
 */
public interface IncVertexCentricComputeFunction<K, VV, EV, M> extends IncVertexCentricFunction<K, VV, EV, M> {

    /**
     * Initialize compute function based on context.
     */
    void init(IncGraphComputeContext<K, VV, EV, M> incGraphContext);

    interface IncGraphComputeContext<K, VV, EV, M> extends IncGraphContext<K, VV, EV, M> {
        /**
         * Partition vertex.
         */
        void collect(IVertex<K, VV> vertex);
    }

}
