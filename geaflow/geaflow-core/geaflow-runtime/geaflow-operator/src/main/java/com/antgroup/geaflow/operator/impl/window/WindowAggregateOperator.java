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

package com.antgroup.geaflow.operator.impl.window;

import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

public class WindowAggregateOperator<KEY, IN, ACC, OUT> extends
        AbstractOneInputOperator<IN, AggregateFunction<IN, ACC, OUT>> {

    private transient Map<KEY, ACC> aggregatingState;
    private KeySelector<IN, KEY> keySelector;

    public WindowAggregateOperator(AggregateFunction<IN, ACC, OUT> aggregateFunction,
                                   KeySelector<IN, KEY> keySelector) {
        super(aggregateFunction);
        this.keySelector = keySelector;
    }

    @Override
    public void open(OpContext opContext) {
        super.open(opContext);
        this.aggregatingState = new HashMap<>();
    }

    @Override
    protected void process(IN value) throws Exception {
        KEY key = keySelector.getKey(value);
        ACC acc = aggregatingState.get(key);

        if (acc == null) {
            acc = this.function.createAccumulator();
        }
        this.function.add(value, acc);
        aggregatingState.put(key, acc);
    }

    @Override
    public void finish() {
        for (ACC acc :aggregatingState.values()) {
            OUT result = this.function.getResult(acc);
            if (result != null) {
                collectValue(result);
            }
        }
        aggregatingState.clear();
        super.finish();
    }

    @VisibleForTesting
    public void processValue(IN value) throws Exception {
        process(value);
    }

}
