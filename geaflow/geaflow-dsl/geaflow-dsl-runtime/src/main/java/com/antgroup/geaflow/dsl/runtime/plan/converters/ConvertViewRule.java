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

package com.antgroup.geaflow.dsl.runtime.plan.converters;

import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicViewRelNode;
import com.antgroup.geaflow.dsl.schema.GeaFlowView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class ConvertViewRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertViewRule();

    private ConvertViewRule() {
        super(LogicalTableScan.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertViewRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);
        GeaFlowView view = scan.getTable().unwrap(GeaFlowView.class);
        return view != null;
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalTableScan scan = (LogicalTableScan) relNode;
        RelTraitSet relTraitSet = relNode.getTraitSet().replace(PhysicConvention.INSTANCE);

        return new PhysicViewRelNode(relNode.getCluster(), relTraitSet, scan.getTable());
    }

}
