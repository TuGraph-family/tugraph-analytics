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

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;

public abstract class AbstractVertexCentricTraversalAlgo<K, VV, EV, M, R,
    FUNC extends VertexCentricTraversalFunction<K, VV, EV, M, R>>
    extends VertexCentricAlgo<K, VV, EV, M> {

    public AbstractVertexCentricTraversalAlgo(long iterations) {
        super(iterations);
    }

    public AbstractVertexCentricTraversalAlgo(long iterations, String name) {
        super(iterations, name);
    }

    public abstract FUNC getTraversalFunction();
}
