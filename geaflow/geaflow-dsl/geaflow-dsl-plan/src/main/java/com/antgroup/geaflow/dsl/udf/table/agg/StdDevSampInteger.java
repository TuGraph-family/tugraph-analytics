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

package com.antgroup.geaflow.dsl.udf.table.agg;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDAF;
import com.antgroup.geaflow.dsl.udf.table.agg.StdDevSampDouble.Accumulator;

@Description(name = "stddev_samp", description = "The stddev function for Integer.")
public class StdDevSampInteger extends UDAF<Integer, Accumulator, Double> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public void accumulate(Accumulator accumulator, Integer input) {
        if (null != input) {
            accumulator.add(input);
        }
    }

    @Override
    public void merge(Accumulator merged, Iterable<Accumulator> accumulators) {
        for (Accumulator accumulator : accumulators) {
            merged.squareSum += accumulator.squareSum;
            merged.sum += accumulator.sum;
            merged.count += accumulator.count;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.squareSum = 0.0;
        accumulator.sum = 0.0;
        accumulator.count = 0L;
    }

    @Override
    public Double getValue(Accumulator accumulator) {
        if (accumulator.count == 0) {
            return 0.0;
        }
        return Math.sqrt((accumulator.squareSum - Math.pow(accumulator.sum, 2) / accumulator.count) / accumulator.count);
    }

}
