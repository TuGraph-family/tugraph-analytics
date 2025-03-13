/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.runtime.plan.converters;

import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicTableFunctionScanRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;

public class ConvertTableFunctionScanRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new ConvertTableFunctionScanRule();

    private ConvertTableFunctionScanRule() {
        super(LogicalTableFunctionScan.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertTableFunctionScanRule.class.getSimpleName());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return super.matches(call);
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalTableFunctionScan tableFunctionScan = (LogicalTableFunctionScan) relNode;

        RelTraitSet relTraitSet =
            tableFunctionScan.getTraitSet()
                .replace(PhysicConvention.INSTANCE);

        return new PhysicTableFunctionScanRelNode(
            tableFunctionScan.getCluster(),
            relTraitSet,
            tableFunctionScan.getInputs(),
            tableFunctionScan.getCall(),
            tableFunctionScan.getElementType(),
            tableFunctionScan.getRowType(),
            tableFunctionScan.getColumnMappings());
    }

}
