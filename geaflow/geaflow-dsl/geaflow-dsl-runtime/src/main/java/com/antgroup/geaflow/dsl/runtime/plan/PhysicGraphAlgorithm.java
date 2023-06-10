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

package com.antgroup.geaflow.dsl.runtime.plan;

import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.rel.GraphAlgorithm;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class PhysicGraphAlgorithm extends GraphAlgorithm implements PhysicRelNode<RuntimeTable> {

    public PhysicGraphAlgorithm(RelOptCluster cluster,
                                   RelTraitSet traits,
                                   RelNode input,
                                   Class<? extends AlgorithmUserFunction> userFunctionClass,
                                   Object[] params) {
        super(cluster, traits, input, userFunctionClass, params);
    }

    @Override
    public GraphAlgorithm copy(RelTraitSet traitSet, RelNode input,
                               Class<? extends AlgorithmUserFunction> userFunctionClass,
                               Object[] params) {
        return new PhysicGraphAlgorithm(input.getCluster(), traitSet, input, userFunctionClass, params);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        PhysicRelNode<RuntimeGraph> input = (PhysicRelNode<RuntimeGraph>) getInput();
        RuntimeGraph inputGraph = input.translate(context);
        return inputGraph.runAlgorithm(this);
    }

    @Override
    public String showSQL() {
        return null;
    }
}
