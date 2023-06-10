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

package com.antgroup.geaflow.dsl.runtime.plan.converters;

import com.antgroup.geaflow.dsl.rel.logical.LogicalConstructGraph;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConstructGraphRelNode;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class ConvertConstructGraphRule extends ConverterRule {

    public static final ConvertConstructGraphRule INSTANCE = new ConvertConstructGraphRule();

    private ConvertConstructGraphRule() {
        super(LogicalConstructGraph.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertConstructGraphRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalConstructGraph constructGraph = (LogicalConstructGraph) rel;

        RelTraitSet relTraitSet = constructGraph.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(constructGraph.getInput(),
            constructGraph.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicConstructGraphRelNode(constructGraph.getCluster(), relTraitSet, convertedInput,
            constructGraph.getLabelNames(), constructGraph.getRowType());
    }
}
