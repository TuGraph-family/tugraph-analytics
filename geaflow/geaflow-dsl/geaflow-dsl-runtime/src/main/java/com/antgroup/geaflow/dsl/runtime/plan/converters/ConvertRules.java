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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.convert.ConverterRule;

public class ConvertRules {

    public static ImmutableList<ConverterRule> TRANSFORM_RULES = ImmutableList.of(
        ConvertAggregateRule.INSTANCE,
        ConvertCorrelateRule.INSTANCE,
        ConvertFilterRule.INSTANCE,
        ConvertTableSortRule.INSTANCE,
        ConvertProjectRule.INSTANCE,
        ConvertTableModifyRule.INSTANCE,
        ConvertTableScanRule.INSTANCE,
        ConvertViewRule.INSTANCE,
        ConvertTableFunctionScanRule.INSTANCE,
        ConvertUnionRule.INSTANCE,
        ConvertValuesRule.INSTANCE,
        ConvertExchangeRule.INSTANCE,
        ConvertGraphScanRule.INSTANCE,
        ConvertGraphMatchRule.INSTANCE,
        ConvertConstructGraphRule.INSTANCE,
        ConvertParameterizedRelNodeRule.INSTANCE,
        ConvertGraphModifyRule.INSTANCE,
        ConvertGraphAlgorithmRule.INSTANCE
    );
}
