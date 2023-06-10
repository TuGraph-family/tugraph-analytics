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

package com.antgroup.geaflow.api.graph.compute;

import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.model.graph.vertex.IVertex;

public interface PGraphCompute<K, VV, EV> {

    /**
     * Returns the vertices of graph.
     */
    PWindowStream<IVertex<K, VV>> getVertices();

    /**
     * Returns the PGraphCompute itself.
     */
    PGraphCompute<K, VV, EV> compute();

    /**
     * Set parallelism of graph compute and return the PGraphCompute itself.
     */
    PGraphCompute<K, VV, EV> compute(int parallelism);

    /**
     * Returns the graph compute type, {@link com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo}.
     */
    GraphExecAlgo getGraphComputeType();

}
