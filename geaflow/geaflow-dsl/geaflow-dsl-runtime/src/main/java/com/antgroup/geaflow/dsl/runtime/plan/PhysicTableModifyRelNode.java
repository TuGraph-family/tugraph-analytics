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
import com.antgroup.geaflow.dsl.runtime.SinkDataView;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class PhysicTableModifyRelNode extends TableModify implements PhysicRelNode<SinkDataView> {

    public PhysicTableModifyRelNode(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RelOptTable table,
                                    Prepare.CatalogReader catalogReader,
                                    RelNode input,
                                    Operation operation,
                                    List<String> updateColumnList,
                                    List<RexNode> sourceExpressionList,
                                    boolean flattened) {
        super(cluster, traitSet, table, catalogReader, input, operation,
            updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhysicTableModifyRelNode(super.getCluster(),
            traitSet,
            super.getTable(),
            super.catalogReader,
            sole(inputs),
            super.getOperation(),
            super.getUpdateColumnList(),
            super.getSourceExpressionList(),
            super.isFlattened());
    }

    @SuppressWarnings("unchecked")
    @Override
    public SinkDataView translate(QueryContext context) {
        GeaFlowTable table = getTable().unwrap(GeaFlowTable.class);
        context.addReferTargetTable(table);

        RuntimeTable runtimeTable = ((PhysicRelNode<RuntimeTable>) getInput()).translate(context);
        return runtimeTable.write(table);
    }

    @Override
    public String showSQL() {
        GeaFlowTable table = super.getTable().unwrap(GeaFlowTable.class);
        return table.toString();
    }
}
