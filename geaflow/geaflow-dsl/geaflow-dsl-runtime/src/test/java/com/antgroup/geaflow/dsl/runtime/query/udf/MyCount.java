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

package com.antgroup.geaflow.dsl.runtime.query.udf;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDAF;
import com.antgroup.geaflow.dsl.common.function.UDAFArguments;
import com.antgroup.geaflow.dsl.runtime.query.udf.MyCount.Accumulator;
import com.antgroup.geaflow.dsl.runtime.query.udf.MyCount.MultiArguments;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Description(name = "count", description = "custom count function for test")
public class MyCount extends UDAF<MultiArguments, Accumulator, Long> {

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(0);
    }

    @Override
    public void accumulate(Accumulator accumulator, MultiArguments input) {
        if (input.getParam(0).toString().equalsIgnoreCase("jim")) {
            accumulator.value += 10000L;
        }
        accumulator.value += (long) input.getParam(1);
    }

    @Override
    public void merge(Accumulator accumulator, Iterable<Accumulator> its) {
        for (Accumulator toMerge : its) {
            accumulator.value += toMerge.value;
        }
    }

    @Override
    public void resetAccumulator(Accumulator accumulator) {
        accumulator.value = 0L;
    }

    @Override
    public Long getValue(Accumulator accumulator) {
        return accumulator.value;
    }

    public static class Accumulator implements Serializable {

        public Accumulator() {
        }

        public long value = 0;

        public Accumulator(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Accumulator{"
                + "value=" + value
                + '}';
        }
    }

    public static class MultiArguments extends UDAFArguments {

        public MultiArguments() {
        }

        @Override
        public List<Class<?>> getParamTypes() {
            List<Class<?>> types = new ArrayList<>();
            types.add(BinaryString.class);
            types.add(Long.class);
            return types;
        }
    }
}

