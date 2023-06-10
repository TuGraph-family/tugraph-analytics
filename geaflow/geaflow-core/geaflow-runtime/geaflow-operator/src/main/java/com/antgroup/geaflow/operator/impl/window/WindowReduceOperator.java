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

import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.operator.base.window.AbstractOneInputOperator;
import com.google.common.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

public class WindowReduceOperator<KEY, T> extends AbstractOneInputOperator<T, ReduceFunction<T>> {

    private final Map<KEY, T> valueState;
    private final KeySelector<T, KEY> keySelector;

    public WindowReduceOperator(ReduceFunction<T> function, KeySelector<T, KEY> keySelector) {
        super(function);
        this.keySelector = keySelector;
        this.valueState = new HashMap<>();
    }

    @Override
    protected void process(T value) throws Exception {
        KEY key = keySelector.getKey(value);
        T oldValue = valueState.get(key);

        T newValue;
        if (oldValue == null) {
            newValue = value;
        } else {
            newValue = function.reduce(oldValue, value);
        }
        valueState.put(key, newValue);
    }

    @Override
    public void finish() {
        for (T value : valueState.values()) {
            if (value != null) {
                collectValue(value);
            }
        }
        super.finish();
        valueState.clear();
    }

    @VisibleForTesting
    public void processValue(T value) throws Exception {
        process(value);
    }
}
