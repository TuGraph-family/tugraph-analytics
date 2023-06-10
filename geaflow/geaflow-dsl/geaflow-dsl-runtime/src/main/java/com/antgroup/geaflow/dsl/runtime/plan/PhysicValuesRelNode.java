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

package com.antgroup.geaflow.dsl.runtime.plan;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.util.GQLRexUtil;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

public class PhysicValuesRelNode extends Values implements PhysicRelNode<RuntimeTable> {

    public PhysicValuesRelNode(RelOptCluster cluster,
                               RelDataType rowType,
                               ImmutableList<ImmutableList<RexLiteral>> tuples,
                               RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicValuesRelNode(getCluster(), getRowType(), getTuples(), traitSet);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        List<Row> values = convertValues();
        return context.getEngineContext().createRuntimeTable(context, values);
    }

    private List<Row> convertValues() {
        List<Row> values = new ArrayList<>();
        for (List<RexLiteral> literals : getTuples()) {
            Object[] fields = new Object[literals.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = GQLRexUtil.getLiteralValue(literals.get(i));
            }
            Row row = ObjectRow.create(fields);
            values.add(row);
        }
        return values;
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("Values(")
            .append(getTuples().stream()
                .map(literals -> "(" + literals.stream().map(literal ->
                    literal.toString()).reduce((a, b) -> a + "," + b).get() + ")")
                .reduce((a, b) -> a + " " + b).get())
            .append(")");
        return sql.toString();
    }
}
