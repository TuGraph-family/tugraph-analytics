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

package com.antgroup.geaflow.api.graph.base.algo;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricComputeFunction;

public abstract class AbstractVertexCentricComputeAlgo<K, VV, EV, M,
    FUNC extends VertexCentricComputeFunction<K, VV, EV, M>> extends VertexCentricAlgo<K, VV, EV, M> {

    public AbstractVertexCentricComputeAlgo(long iterations) {
        super(iterations);
    }

    public AbstractVertexCentricComputeAlgo(long iterations, String name) {
        super(iterations, name);
    }

    public abstract FUNC getComputeFunction();
}
