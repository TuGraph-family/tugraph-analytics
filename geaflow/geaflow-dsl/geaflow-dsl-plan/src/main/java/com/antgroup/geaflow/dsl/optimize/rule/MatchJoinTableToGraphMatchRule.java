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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class MatchJoinTableToGraphMatchRule extends AbstractJoinToGraphRule {

    public static final MatchJoinTableToGraphMatchRule INSTANCE = new MatchJoinTableToGraphMatchRule();

    private MatchJoinTableToGraphMatchRule() {
        super(operand(LogicalJoin.class,
            operand(LogicalProject.class, operand(LogicalGraphMatch.class, any())),
            operand(LogicalTableScan.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        processMatchTableJoinToGraphMatch(
            call,
            call.rel(0),
            call.rel(2),
            call.rel(1),
            call.rel(3)
        );
    }


}
