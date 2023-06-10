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

package com.antgroup.geaflow.api.function.base;

import com.antgroup.geaflow.api.function.Function;

public interface AggregateFunction<IN, ACC, OUT> extends Function {

    /**
     * Create aggregate accumulator for aggregate function to store the aggregate value.
     */
    ACC createAccumulator();

    /**
     * Accumulate the input to the accumulator.
     */
    void add(IN value, ACC accumulator);

    /**
     * Get aggregate result from the accumulator.
     */
    OUT getResult(ACC accumulator);

    /**
     * Merge a with b.
     */
    ACC merge(ACC a, ACC b);

}
