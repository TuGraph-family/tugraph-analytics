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
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.util.ExpressionUtil;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class PhysicTableFunctionScanRelNode extends TableFunctionScan implements PhysicRelNode<RuntimeTable> {

    public PhysicTableFunctionScanRelNode(RelOptCluster cluster,
                                          RelTraitSet traits,
                                          List<RelNode> inputs, RexNode rexCall,
                                          Type elementType, RelDataType rowType,
                                          Set<RelColumnMapping> columnMappings) {
        super(cluster, traits, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public TableFunctionScan copy(RelTraitSet traitSet, List<RelNode> inputs,
                                  RexNode rexCall,
                                  Type elementType, RelDataType rowType,
                                  Set<RelColumnMapping> columnMappings) {
        return new PhysicTableFunctionScanRelNode(
                super.getCluster(),
                traitSet,
                inputs,
                rexCall,
                elementType,
                rowType,
                columnMappings);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        throw new GeaFlowDSLException("Table function is not support");
    }

    @Override
    public String showSQL() {
        return ExpressionUtil.showExpression(getCall(), null, null);
    }

}
