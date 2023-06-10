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

package com.antgroup.geaflow.dsl.udf.table.agg;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDAF;
import com.antgroup.geaflow.dsl.udf.table.agg.AvgDouble.Accumulator;
import java.io.Serializable;

@Description(name = "avg", description = "The avg function for double input.")
public class AvgDouble extends UDAF<Double, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, Double input) {
        if (null != input) {
            accumulator.sum += input;
            accumulator.count ++;
        }
    }

    @Override
    public void merge(Accumulator merged, Iterable<Accumulator> accumulators) {
        for (Accumulator accumulator : accumulators) {
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.sum = 0.0;
        accumulator.count = 0L;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        return accumulator.count == 0 ? null : (accumulator.sum / (double) accumulator.count);
    }

    public static class Accumulator implements Serializable {
        public double sum = 0.0;
        public long count = 0;
    }
}
