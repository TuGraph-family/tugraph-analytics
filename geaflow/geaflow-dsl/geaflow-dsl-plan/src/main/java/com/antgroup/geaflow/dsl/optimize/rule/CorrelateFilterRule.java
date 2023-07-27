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

package com.antgroup.geaflow.dsl.optimize.rule;

import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;

public class CorrelateFilterRule extends RelOptRule {

    public static final CorrelateFilterRule INSTANCE = new CorrelateFilterRule();

    private CorrelateFilterRule() {
        super(operand(Correlate.class, any()));
    }


    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalCorrelate correlate = call.rel(0);
        return !(GQLRelUtil.toRel(correlate.getRight()) instanceof LogicalTableFunctionScan);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        LogicalCorrelate correlate = relOptRuleCall.rel(0);
        RelNode rightInput = GQLRelUtil.toRel(correlate.getRight());
        while (!(rightInput instanceof LogicalTableFunctionScan)) {
            Preconditions.checkArgument(rightInput.getInputs().size() == 1);
            rightInput = GQLRelUtil.toRel(rightInput.getInput(0));
        }
        relOptRuleCall.transformTo(
            correlate.copy(correlate.getTraitSet(), correlate.getLeft(), rightInput,
                correlate.getCorrelationId(), correlate.getRequiredColumns(), correlate.getJoinType()));
    }
}
