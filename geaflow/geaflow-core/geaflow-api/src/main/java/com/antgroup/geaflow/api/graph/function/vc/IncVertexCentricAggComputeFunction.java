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

import com.antgroup.geaflow.api.graph.function.aggregate.VertexCentricAggContextFunction;

/**
 * Interface for incremental vertex centric compute function with graph aggregation.
 * @param <K> The id type of vertex/edge.
 * @param <VV> The value type of vertex.
 * @param <EV> The value type of edge.
 * @param <M> The message type during iterations.
 * @param <I> The type of aggregate input iterm.
 * @param <GR> The type of aggregate global result.
 */
public interface IncVertexCentricAggComputeFunction<K, VV, EV, M, I, GR>
    extends IncVertexCentricComputeFunction<K, VV, EV, M>, VertexCentricAggContextFunction<I, GR> {

}
