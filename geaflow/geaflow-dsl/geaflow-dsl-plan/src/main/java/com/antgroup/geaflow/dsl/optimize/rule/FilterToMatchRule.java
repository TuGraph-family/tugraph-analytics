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
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.MatchFilter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;

public class FilterToMatchRule extends RelOptRule {

    public static final FilterToMatchRule INSTANCE = new FilterToMatchRule();

    private FilterToMatchRule() {
        super(operand(LogicalFilter.class,
                operand(LogicalGraphMatch.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalGraphMatch graphMatch = call.rel(1);

        IMatchNode pathPattern = graphMatch.getPathPattern();

        MatchFilter matchFilter = MatchFilter.create(pathPattern,
            filter.getCondition(), pathPattern.getPathSchema());
        LogicalGraphMatch newMatch = (LogicalGraphMatch) graphMatch.copy(matchFilter);
        call.transformTo(newMatch);
    }
}
