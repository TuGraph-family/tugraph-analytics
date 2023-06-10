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

import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicSortRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

public class ConvertTableSortRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertTableSortRule();

    private ConvertTableSortRule() {
        super(LogicalSort.class, Convention.NONE, PhysicConvention.INSTANCE,
            ConvertTableSortRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;
        RelNode input = sort.getInput();

        return new PhysicSortRelNode(sort.getCluster(),
            sort.getTraitSet().replace(PhysicConvention.INSTANCE),
            convert(input, input.getTraitSet().replace(PhysicConvention.INSTANCE)),
            sort.getCollation(), sort.offset, sort.fetch);
    }
}
