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
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class PhysicTableScanRelNode extends TableScan implements PhysicRelNode<RuntimeTable> {

    public PhysicTableScanRelNode(RelOptCluster cluster,
                                  RelTraitSet traitSet,
                                  RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicTableScanRelNode(getCluster(), traitSet, getTable());
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        GeaFlowTable geaFlowTable = table.unwrap(GeaFlowTable.class);
        RuntimeTable runtimeTable = context.getRuntimeTable(geaFlowTable.getName());
        if (runtimeTable == null) {
            Expression pushFilter = context.getPushFilter();
            context.addReferSourceTable(geaFlowTable);
            runtimeTable = context.getEngineContext().createRuntimeTable(context, geaFlowTable, pushFilter);
            context.putRuntimeTable(geaFlowTable.getName(), runtimeTable);
        }
        return runtimeTable;
    }

    @Override
    public String showSQL() {
        GeaFlowTable table = super.getTable().unwrap(GeaFlowTable.class);
        return table.toString();
    }
}
