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
import com.antgroup.geaflow.dsl.udf.table.agg.MinLong.Accumulator;
import java.io.Serializable;

@Description(name = "min", description = "The min function for bigint.")
public class MinLong extends UDAF<Long, Accumulator, Long> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(null);
    }

    @Override
    public void accumulate(Accumulator accumulator, Long input) {
        if (null == accumulator.value
            || (null != input && accumulator.value.compareTo(input) > 0)) {
            accumulator.value = input;
        }
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            if (accumulator.value == null
                || (null != toMerge.value && accumulator.value.compareTo(toMerge.value) > 0)) {
                accumulator.value = toMerge.value;
            }
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = null;
    }

    @Override
    public Long getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {

        public Accumulator() {
        }

        public Long value;

        public Accumulator(Long value) {
            this.value = value;
        }
    }
}
