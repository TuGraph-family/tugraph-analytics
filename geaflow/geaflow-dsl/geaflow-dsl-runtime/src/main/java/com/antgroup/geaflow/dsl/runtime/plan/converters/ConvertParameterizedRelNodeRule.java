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

import com.antgroup.geaflow.dsl.rel.logical.LogicalParameterizedRelNode;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicParameterizedRelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class ConvertParameterizedRelNodeRule extends ConverterRule {

    public static final ConvertParameterizedRelNodeRule INSTANCE = new ConvertParameterizedRelNodeRule();

    private ConvertParameterizedRelNodeRule() {
        super(LogicalParameterizedRelNode.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertParameterizedRelNodeRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalParameterizedRelNode parameterizedRelNode = (LogicalParameterizedRelNode) rel;

        RelNode parameter = parameterizedRelNode.getParameterNode();
        RelNode convertedParameter = convert(parameter,
            parameter.getTraitSet().replace(PhysicConvention.INSTANCE));

        RelNode query = parameterizedRelNode.getQueryNode();
        RelNode convertedQuery = convert(query,
            query.getTraitSet().replace(PhysicConvention.INSTANCE));

        RelTraitSet traitSet = rel.getTraitSet().replace(PhysicConvention.INSTANCE);
        return new PhysicParameterizedRelNode(
            rel.getCluster(),
            traitSet,
            convertedParameter,
            convertedQuery);
    }
}
