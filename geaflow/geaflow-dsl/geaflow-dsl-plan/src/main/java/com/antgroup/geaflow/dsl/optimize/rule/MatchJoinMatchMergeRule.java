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

import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphMatch;
import com.antgroup.geaflow.dsl.rel.match.IMatchNode;
import com.antgroup.geaflow.dsl.rel.match.MatchJoin;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class MatchJoinMatchMergeRule extends AbstractJoinToGraphRule {

    public static final MatchJoinMatchMergeRule INSTANCE = new MatchJoinMatchMergeRule();

    private MatchJoinMatchMergeRule() {
        super(operand(LogicalJoin.class,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        if (!join.getJoinType().equals(JoinRelType.INNER)) {
            // non-INNER joins is not supported.
            return false;
        }
        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);
        return isSingleChainFromGraphMatch(leftInput)
            && isSingleChainFromGraphMatch(rightInput);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode leftInput = call.rel(1);
        RelNode leftGraphMatchProject = null;
        RelNode leftGraphMatch = leftInput;
        while (leftGraphMatch != null && !(leftGraphMatch instanceof LogicalGraphMatch)) {
            leftGraphMatchProject = leftGraphMatch;
            leftGraphMatch = GQLRelUtil.toRel(leftGraphMatch.getInput(0));
        }
        RelNode rightInput = call.rel(2);
        RelNode rightGraphMatchProject = null;
        RelNode rightGraphMatch = rightInput;
        while (rightGraphMatch != null && !(rightGraphMatch instanceof LogicalGraphMatch)) {
            rightGraphMatchProject = rightGraphMatch;
            rightGraphMatch = GQLRelUtil.toRel(rightGraphMatch.getInput(0));
        }
        assert leftGraphMatch != null && rightGraphMatch != null;

        RelBuilder relBuilder = call.builder();
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        List<RexNode> rexLeftNodeMap = new ArrayList<>();
        List<RexNode> rexRightNodeMap = new ArrayList<>();
        IMatchNode leftPathPattern = ((GraphMatch)leftGraphMatch).getPathPattern();
        leftPathPattern = concatToMatchNode(relBuilder, leftInput, leftGraphMatch, leftPathPattern, rexLeftNodeMap);
        IMatchNode rightPathPattern = ((GraphMatch)rightGraphMatch).getPathPattern();
        rightPathPattern = concatToMatchNode(relBuilder, rightInput, rightGraphMatch, rightPathPattern, rexRightNodeMap);
        if (leftPathPattern == null || rightPathPattern == null) {
            return;
        }
        LogicalJoin join = call.rel(0);
        MatchJoin newPathPattern = MatchJoin.create(join.getCluster(), join.getTraitSet(),
            leftPathPattern, rightPathPattern, join.getCondition(), JoinRelType.INNER);
        GraphMatch newGraphMatch = ((GraphMatch)leftGraphMatch).copy(newPathPattern);

        JoinInfo joinInfo = join.analyzeCondition();
        List<RexNode> joinConditions = new ArrayList<>();
        if (newGraphMatch.getPathPattern() instanceof MatchJoin) {
            MatchJoin matchJoin = (MatchJoin) newGraphMatch.getPathPattern();
            for (int i = 0; i < joinInfo.leftKeys.size(); i++) {
                int left = joinInfo.leftKeys.get(i);
                int right = joinInfo.rightKeys.get(i);
                RexNode leftNode = rexLeftNodeMap.get(left);
                RexNode rightNode = rexRightNodeMap.get(right);
                rightNode = adjustRightRexNodes(Collections.singletonList(rightNode), newGraphMatch,
                    relBuilder, leftPathPattern, rightPathPattern).get(0);
                SqlOperator equalsOperator = SqlStdOperatorTable.EQUALS;
                RexNode condition = relBuilder.getRexBuilder().makeCall(equalsOperator,
                    leftNode, rightNode);
                joinConditions.add(condition);
            }
            RexNode newCondition = RexUtil.composeConjunction(rexBuilder, joinConditions);
            newGraphMatch = newGraphMatch.copy(matchJoin.copy(matchJoin.getTraitSet(),
                newCondition, matchJoin.getLeft(), matchJoin.getRight(), matchJoin.getJoinType()));
        }

        List<RexNode> newProjects = new ArrayList<>();
        if (rexLeftNodeMap.size() > 0) {
            newProjects.addAll(rexLeftNodeMap);
        } else {
            assert leftGraphMatchProject != null;
            newProjects.addAll(adjustLeftRexNodes(
                ((LogicalProject)leftGraphMatchProject).getProjects(), newGraphMatch, relBuilder));
        }
        if (rexRightNodeMap.size() > 0) {
            newProjects.addAll(adjustRightRexNodes(rexRightNodeMap, newGraphMatch, relBuilder,
                leftPathPattern, rightPathPattern));
        } else {
            assert rightGraphMatchProject != null;
            newProjects.addAll(adjustRightRexNodes(
                ((LogicalProject)rightGraphMatchProject).getProjects(), newGraphMatch, relBuilder,
                leftPathPattern, rightPathPattern));
        }
        List<String> fieldNames = getNewFieldNames(newProjects.size(), new HashSet<>());
        RelNode tail = LogicalProject.create(newGraphMatch, newProjects, fieldNames);

        // Complete the Join projection.
        final RelNode finalTail = tail;
        List<RexNode> joinProjects = IntStream.range(0, newProjects.size())
            .mapToObj(i -> rexBuilder.makeInputRef(finalTail, i)).collect(Collectors.toList());
        tail = LogicalProject.create(tail, joinProjects, join.getRowType());
        // Add remain filter.
        RexNode remainFilter = joinInfo.getRemaining(join.getCluster().getRexBuilder());
        if (remainFilter != null && !remainFilter.isAlwaysTrue()) {
            tail = LogicalFilter.create(tail, remainFilter);
        }
        call.transformTo(tail);
    }
}
