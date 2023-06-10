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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricTraversalFunction;
import com.antgroup.geaflow.api.graph.traversal.VertexCentricTraversal;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import java.util.Objects;

public class GeaFlowAlgorithmTraversal extends VertexCentricTraversal<Object, Row, Row, Object, Row> {

    private final AlgorithmUserFunction userFunction;
    private final Object[] params;

    private final GraphSchema graphSchema;

    public GeaFlowAlgorithmTraversal(AlgorithmUserFunction userFunction, int maxTraversal,
                                     Object[] params, GraphSchema graphSchema) {
        super(maxTraversal);
        this.userFunction = Objects.requireNonNull(userFunction);
        this.params = Objects.requireNonNull(params);
        this.graphSchema = Objects.requireNonNull(graphSchema);
    }

    @Override
    public VertexCentricCombineFunction<Object> getCombineFunction() {
        return null;
    }

    @Override
    public IEncoder<Object> getMessageEncoder() {
        return null;
    }

    @Override
    public VertexCentricTraversalFunction<Object, Row, Row, Object, Row> getTraversalFunction() {
        return new GeaFlowAlgorithmTraversalFunction(graphSchema, userFunction, params);
    }
}
