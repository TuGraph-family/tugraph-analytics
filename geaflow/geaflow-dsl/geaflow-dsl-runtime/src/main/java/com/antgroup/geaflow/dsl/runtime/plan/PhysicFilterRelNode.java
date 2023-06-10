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

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RDataView;
import com.antgroup.geaflow.dsl.runtime.RDataView.ViewType;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.ExpressionTranslator;
import com.antgroup.geaflow.dsl.runtime.function.table.WhereFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.WhereFunctionImpl;
import com.antgroup.geaflow.dsl.util.ExpressionUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class PhysicFilterRelNode extends Filter implements PhysicRelNode<RDataView> {

    public PhysicFilterRelNode(RelOptCluster cluster,
                               RelTraitSet traits,
                               RelNode child,
                               RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PhysicFilterRelNode(super.getCluster(), traitSet, input, condition);
    }

    @Override
    public RDataView translate(QueryContext context) {
        ExpressionTranslator translator = ExpressionTranslator.of(getInput().getRowType());
        Expression condition = translator.translate(getCondition());
        Expression preFilter = context.getPushFilter();
        if (getInput() instanceof TableScan) {
            context.setPushFilter(condition);
        }
        RDataView dataView = ((PhysicRelNode<?>) getInput()).translate(context);
        context.setPushFilter(preFilter);
        RuntimeTable runtimeTable;
        if (dataView.getType() == ViewType.TABLE) {
            runtimeTable = (RuntimeTable) dataView;
        } else if (dataView.getType() == ViewType.GRAPH) {
            RuntimeGraph runtimeGraph = (RuntimeGraph) dataView;
            runtimeTable = runtimeGraph.getPathTable();
        } else {
            throw new GeaFlowDSLException("DataView: " + dataView.getType() + " cannot support filter");
        }
        WhereFunction whereFunction = new WhereFunctionImpl(condition);
        return runtimeTable.filter(whereFunction);
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("WHERE ");
        sql.append(ExpressionUtil.showExpression(condition, null,
            getInput().getRowType()));
        return sql.toString();
    }
}
