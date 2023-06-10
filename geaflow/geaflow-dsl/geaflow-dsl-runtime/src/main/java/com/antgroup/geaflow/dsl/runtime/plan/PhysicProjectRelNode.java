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

import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RDataView;
import com.antgroup.geaflow.dsl.runtime.RDataView.ViewType;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.ExpressionTranslator;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunction;
import com.antgroup.geaflow.dsl.runtime.function.table.ProjectFunctionImpl;
import com.antgroup.geaflow.dsl.util.ExpressionUtil;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class PhysicProjectRelNode extends Project implements PhysicRelNode<RuntimeTable> {

    public PhysicProjectRelNode(RelOptCluster cluster,
                                RelTraitSet traits,
                                RelNode input,
                                List<? extends RexNode> projects,
                                RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public Project copy(RelTraitSet traitSet,
                        RelNode input,
                        List<RexNode> projects,
                        RelDataType rowType) {
        return new PhysicProjectRelNode(super.getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        List<Expression> projects = ExpressionTranslator.of(getInput().getRowType()).translate(getProjects());
        ProjectFunction projectFunction = new ProjectFunctionImpl(projects);

        RDataView dataView = ((PhysicRelNode<?>) getInput()).translate(context);
        if (dataView.getType() == ViewType.TABLE) {
            RuntimeTable runtimeTable = (RuntimeTable) dataView;
            return runtimeTable.project(projectFunction);
        } else { // project path for graph.
            RuntimeGraph runtimeGraph = (RuntimeGraph) dataView;
            return runtimeGraph.getPathTable().project(projectFunction);
        }
    }

    @Override
    public String showSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT \n");
        for (int i = 0; i < exps.size(); i++) {
            if (i > 0) {
                sql.append("\n");
            }
            sql.append(ExpressionUtil.showExpression(exps.get(i), null,
                input.getRowType()));
        }
        return sql.toString();
    }
}
