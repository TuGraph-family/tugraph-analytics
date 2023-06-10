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

import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicGraphMatchRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class ConvertGraphMatchRule extends ConverterRule {

    public static final ConvertGraphMatchRule INSTANCE = new ConvertGraphMatchRule();

    private ConvertGraphMatchRule() {
        super(LogicalGraphMatch.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertGraphMatchRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalGraphMatch graphMatch = (LogicalGraphMatch) rel;

        RelTraitSet relTraitSet = graphMatch.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(graphMatch.getInput(),
            graphMatch.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicGraphMatchRelNode(graphMatch.getCluster(), relTraitSet,
            convertedInput, graphMatch.getPathPattern(), graphMatch.getRowType());
    }
}
