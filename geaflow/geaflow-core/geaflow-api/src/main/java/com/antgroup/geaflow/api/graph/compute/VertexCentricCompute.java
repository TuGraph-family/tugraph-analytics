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


import com.antgroup.geaflow.api.graph.base.algo.AbstractVertexCentricComputeAlgo;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;

public abstract class VertexCentricCompute<K, VV, EV, M>
    extends AbstractVertexCentricComputeAlgo<K, VV, EV, M, VertexCentricComputeFunction<K, VV, EV, M>> {

    public VertexCentricCompute(long iterations) {
        super(iterations);
    }

    public VertexCentricCompute(long iterations, String name) {
        super(iterations, name);
    }
}
