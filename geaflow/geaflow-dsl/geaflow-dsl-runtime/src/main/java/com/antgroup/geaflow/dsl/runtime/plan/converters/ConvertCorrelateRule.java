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
import com.antgroup.geaflow.dsl.runtime.plan.PhysicCorrelateRelNode;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCorrelate;

public class ConvertCorrelateRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertCorrelateRule();

    private ConvertCorrelateRule() {
        super(LogicalCorrelate.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertCorrelateRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCorrelate join = call.rel(0);
        RelNode right = join.getRight();
        return GQLRelUtil.findTableFunctionScan(right);
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalCorrelate join = (LogicalCorrelate) relNode;
        RelTraitSet relTraitSet = relNode.getTraitSet().replace(PhysicConvention.INSTANCE);

        RelNode convertedLeft = convert(join.getLeft(),
            join.getLeft().getTraitSet().replace(PhysicConvention.INSTANCE));
        RelNode convertedRight = convert(join.getRight(),
            join.getRight().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicCorrelateRelNode(join.getCluster(), relTraitSet,
            convertedLeft, convertedRight, join.getCorrelationId(),
            join.getRequiredColumns(), join.getJoinType());
    }
}
