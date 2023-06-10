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

import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public class PhysicGraphMatchRelNode extends GraphMatch implements PhysicRelNode<RuntimeGraph> {

    public PhysicGraphMatchRelNode(RelOptCluster cluster,
                                   RelTraitSet traits,
                                   RelNode input,
                                   IMatchNode pathPattern,
                                   RelDataType rowType) {
        super(cluster, traits, input, pathPattern, rowType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RuntimeGraph translate(QueryContext context) {
        PhysicRelNode<RuntimeGraph> input = (PhysicRelNode<RuntimeGraph>) getInput();
        RuntimeGraph inputGraph = input.translate(context);
        return inputGraph.traversal(this);
    }

    @Override
    public GraphMatch copy(RelTraitSet traitSet, RelNode input, IMatchNode pathPattern, RelDataType rowType) {
        return new PhysicGraphMatchRelNode(getCluster(), traitSet, input, pathPattern, rowType);
    }

    @Override
    public String showSQL() {
        return null;
    }
}
