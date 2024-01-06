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

import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class MatchJoinTableToGraphMatchRule extends AbstractJoinToGraphRule {

    public static final MatchJoinTableToGraphMatchRule INSTANCE = new MatchJoinTableToGraphMatchRule();

    private MatchJoinTableToGraphMatchRule() {
        super(operand(LogicalJoin.class,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        if (!isSupportJoinType(join.getJoinType())) {
            // non-INNER joins is not supported.
            return false;
        }
        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);
        return isSingleChainFromGraphMatch(leftInput)
            && isSingleChainFromLogicalTableScan(rightInput);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode leftInput = call.rel(1);
        RelNode graphMatchProject = null;
        RelNode leftGraphMatch = leftInput;
        while (leftGraphMatch != null && !(leftGraphMatch instanceof LogicalGraphMatch)) {
            graphMatchProject = leftGraphMatch;
            leftGraphMatch = GQLRelUtil.toRel(leftGraphMatch.getInput(0));
        }
        RelNode rightInput = call.rel(2);
        RelNode rightTableScan = rightInput;
        while (!(rightTableScan instanceof LogicalTableScan)) {
            rightTableScan = GQLRelUtil.toRel(rightTableScan.getInput(0));
        }
        RelNode tail = processGraphMatchJoinTable(
            call,
            call.rel(0),
            (LogicalGraphMatch)leftGraphMatch,
            (LogicalProject)graphMatchProject,
            (LogicalTableScan)rightTableScan,
            leftInput, leftGraphMatch, rightInput, rightTableScan, true
        );
        if (tail == null) {
            return;
        }
        call.transformTo(tail);
    }


}
