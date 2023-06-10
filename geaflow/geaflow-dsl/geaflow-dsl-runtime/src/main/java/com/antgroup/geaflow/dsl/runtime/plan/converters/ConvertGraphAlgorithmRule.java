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

import com.antgroup.geaflow.dsl.rel.logical.LogicalGraphAlgorithm;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicGraphAlgorithm;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public class ConvertGraphAlgorithmRule extends ConverterRule {

    public static final ConvertGraphAlgorithmRule INSTANCE = new ConvertGraphAlgorithmRule();

    private ConvertGraphAlgorithmRule() {
        super(LogicalGraphAlgorithm.class, Convention.NONE,
            PhysicConvention.INSTANCE, ConvertGraphAlgorithmRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalGraphAlgorithm graphAlgorithm = (LogicalGraphAlgorithm) rel;

        RelTraitSet relTraitSet = graphAlgorithm.getTraitSet().replace(PhysicConvention.INSTANCE);
        RelNode convertedInput = convert(graphAlgorithm.getInput(),
            graphAlgorithm.getInput().getTraitSet().replace(PhysicConvention.INSTANCE));

        return new PhysicGraphAlgorithm(graphAlgorithm.getCluster(), relTraitSet,
            convertedInput, graphAlgorithm.getUserFunctionClass(), graphAlgorithm.getParams());
    }
}
