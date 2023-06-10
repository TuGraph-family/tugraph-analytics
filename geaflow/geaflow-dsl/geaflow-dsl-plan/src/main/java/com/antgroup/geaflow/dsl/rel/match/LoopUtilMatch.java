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

package com.antgroup.geaflow.dsl.rel.match;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.rel.MatchNodeVisitor;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

public class LoopUtilMatch extends SingleRel implements SingleMatchNode {

    private final IMatchNode loopBody;

    private final int minLoopCount;

    private final int maxLoopCount;

    private final RexNode utilCondition;

    private final PathRecordType pathType;

    protected LoopUtilMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                            IMatchNode loopBody, RexNode utilCondition,
                            int minLoopCount, int maxLoopCount,
                            PathRecordType pathType) {
        super(cluster, traitSet, input);
        this.loopBody = Objects.requireNonNull(loopBody);
        this.utilCondition = Objects.requireNonNull(utilCondition);
        this.minLoopCount = minLoopCount;
        this.maxLoopCount = maxLoopCount;
        this.pathType = Objects.requireNonNull(pathType);
        this.rowType = pathType;
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return loopBody.getNodeType();
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitLoopMatch(this);
    }

    public IMatchNode getLoopBody() {
        return loopBody;
    }

    public RexNode getUtilCondition() {
        return utilCondition;
    }

    public int getMinLoopCount() {
        return minLoopCount;
    }

    public int getMaxLoopCount() {
        return maxLoopCount;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("body", loopBody)
            .item("minLoopCount", minLoopCount)
            .item("maxLoopCount", maxLoopCount)
            .item("condition", utilCondition);
    }

    @Override
    public IMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        return new LoopUtilMatch(getCluster(), getTraitSet(), sole(inputs), loopBody,
            utilCondition, minLoopCount, maxLoopCount, pathSchema);
    }

    @Override
    public LoopUtilMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LoopUtilMatch(getCluster(), traitSet, sole(inputs), loopBody, utilCondition,
            minLoopCount, maxLoopCount, pathType);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        GQLRelUtil.applyRexShuffleToTree(loopBody, shuttle);
        RexNode newUtilCondition = utilCondition.accept(shuttle);
        return new LoopUtilMatch(getCluster(), getTraitSet(), input, loopBody, newUtilCondition,
            minLoopCount, maxLoopCount, pathType);
    }


    public static LoopUtilMatch create(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                       IMatchNode loopBody, RexNode utilCondition, int minLoopCount,
                                       int maxLoopCount, PathRecordType pathType) {
        return new LoopUtilMatch(cluster, traitSet, input, loopBody, utilCondition, minLoopCount,
            maxLoopCount, pathType);
    }
}
