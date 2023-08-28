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

import static com.antgroup.geaflow.dsl.util.GQLRelUtil.match;

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.rel.MatchNodeVisitor;
import com.antgroup.geaflow.dsl.util.GQLRelUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

public class VertexMatch extends AbstractRelNode implements SingleMatchNode, IMatchLabel {

    private RelNode input;

    private final String label;

    private final ImmutableSet<String> vertexTypes;

    private final PathRecordType pathType;

    private final RelDataType nodeType;

    /**
     * The filter pushed down to the first vertex match.
     */
    private RexNode pushDownFilter;

    public VertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       String label, Collection<String> vertexTypes, RelDataType nodeType,
                       PathRecordType pathType) {
        this(cluster, traitSet, input, label, vertexTypes, nodeType, pathType, null);
    }

    public VertexMatch(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                       String label, Collection<String> vertexTypes, RelDataType nodeType,
                       PathRecordType pathType, RexNode pushDownFilter) {
        super(cluster, traitSet);
        this.input = input;
        this.label = label;
        this.vertexTypes = ImmutableSet.copyOf(vertexTypes);

        if (input != null && !(GQLRelUtil.toRel(input) instanceof SubQueryStart)
            && match(input).getNodeType().getSqlTypeName() != SqlTypeName.EDGE) {
            throw new GeaFlowDSLException("Illegal input type: " + match(input).getNodeType().getSqlTypeName()
                + " for: " + getRelTypeName() + ", should be: " + SqlTypeName.EDGE);
        }
        this.rowType = Objects.requireNonNull(pathType);
        this.pathType = Objects.requireNonNull(pathType);
        this.nodeType = Objects.requireNonNull(nodeType);
        this.pushDownFilter = pushDownFilter;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public Set<String> getTypes() {
        return vertexTypes;
    }

    @Override
    public List<RelNode> getInputs() {
        if (input == null) {
            return Collections.emptyList();
        }
        return ImmutableList.of(input);
    }

    @Override
    public RelNode getInput() {
        return input;
    }

    public RexNode getPushDownFilter() {
        return pushDownFilter;
    }

    @Override
    public SingleMatchNode copy(List<RelNode> inputs, PathRecordType pathSchema) {
        assert inputs.size() <= 1;
        RelNode input = inputs.isEmpty() ? null : inputs.get(0);
        return new VertexMatch(getCluster(), traitSet, input, label,
            vertexTypes, nodeType, pathSchema, pushDownFilter);
    }

    @Override
    public VertexMatch copy(RelTraitSet traitSet, List<RelNode> inputs) {
        RelNode input = GQLRelUtil.oneInput(inputs);
        return new VertexMatch(getCluster(), getTraitSet(), input,
            label, vertexTypes, nodeType, pathType, pushDownFilter);
    }

    public VertexMatch copy(RexNode pushDownFilter) {
        return new VertexMatch(getCluster(), getTraitSet(), input,
            label, vertexTypes, nodeType, pathType, pushDownFilter);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("input", input)
            .item("label", label)
            .item("vertexTypes", vertexTypes);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        assert ordinalInParent == 0;
        this.input = p;
    }

    @Override
    protected RelDataType deriveRowType() {
        throw new UnsupportedOperationException();
    }

    public static VertexMatch create(RelOptCluster cluster, SingleMatchNode input, String label,
                                     List<String> vertexTypes, RelDataType nodeType, PathRecordType pathType) {
        return new VertexMatch(cluster, cluster.traitSet(), input, label, vertexTypes, nodeType, pathType);
    }

    @Override
    public PathRecordType getPathSchema() {
        return pathType;
    }

    @Override
    public RelDataType getNodeType() {
        return nodeType;
    }

    @Override
    public <T> T accept(MatchNodeVisitor<T> visitor) {
        return visitor.visitVertexMatch(this);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        if (pushDownFilter != null) {
            RexNode newPushDownFilter = pushDownFilter.accept(shuttle);
            return copy(newPushDownFilter);
        }
        return this;
    }
}
