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

package com.antgroup.geaflow.dsl.optimize;

import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

public class GQLOptimizer {

    private final Context context;

    private final List<RuleGroup> ruleGroups = new ArrayList<>();

    private int times = 3;

    public GQLOptimizer(Context context) {
        this.context = context;
    }

    public GQLOptimizer() {
        this(Contexts.empty());
    }

    public void addRuleGroup(RuleGroup ruleGroup) {
        if (!ruleGroup.isEmpty()) {
            ruleGroups.add(ruleGroup);
            Collections.sort(ruleGroups, Collections.reverseOrder());
        }
    }

    public int setTimes(int newTimes) {
        int oldTimes = this.times;
        this.times = newTimes;
        return oldTimes;
    }

    public RelNode optimize(RelNode root) {
        return optimize(root, this.times);
    }

    public RelNode optimize(RelNode root, int runTimes) {
        RelNode optimizedNode = root;
        for (int i = 0; i < runTimes ; i++) {
            for (RuleGroup rules : ruleGroups) {
                optimizedNode = applyRules(rules, optimizedNode);
            }
        }
        return optimizedNode;
    }

    private RelNode applyRules(RuleGroup rules, RelNode node) {
        // optimize rel node
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        for (RelOptRule relOptRule : rules) {
            builder.addRuleInstance(relOptRule);
        }
        HepPlanner planner = new HepPlanner(builder.build(), context);
        planner.setRoot(node);
        RelNode optimizedNode = planner.findBestExp();
        // optimize node in match or sub-query.
        return applyRulesOnChildren(rules, optimizedNode);
    }

    private RelNode applyRulesOnChildren(RuleGroup rules, RelNode node) {
        List<RelNode> newInputs = node.getInputs()
            .stream()
            .map(input -> applyRulesOnChildren(rules, input))
            .collect(Collectors.toList());

        if (node instanceof GraphMatch) {
            GraphMatch match = (GraphMatch) node;
            IMatchNode newPathPattern = (IMatchNode) applyRules(rules, match.getPathPattern());
            assert newInputs.size() == 1;
            return match.copy(match.getTraitSet(), newInputs.get(0), newPathPattern, match.getRowType());
        }
        RelNode newNode = node.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                RelNode subNode = subQuery.rel;
                RelNode newSubNode = applyRules(rules, subNode);
                return subQuery.clone(newSubNode);
            }
        });
        return newNode.copy(newNode.getTraitSet(), newInputs);
    }
}
