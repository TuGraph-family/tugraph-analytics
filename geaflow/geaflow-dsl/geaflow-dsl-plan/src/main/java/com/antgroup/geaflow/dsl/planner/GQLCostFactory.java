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

package com.antgroup.geaflow.dsl.planner;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class GQLCostFactory implements RelOptCostFactory {

    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
        return new GQLCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
        return GQLCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
        return GQLCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
        return GQLCost.TINY;
    }

    public RelOptCost makeZeroCost() {
        return GQLCost.ZERO;
    }

}
