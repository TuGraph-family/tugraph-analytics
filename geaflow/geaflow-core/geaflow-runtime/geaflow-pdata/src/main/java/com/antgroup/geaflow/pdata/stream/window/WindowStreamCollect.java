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

package com.antgroup.geaflow.pdata.stream.window;

import com.antgroup.geaflow.api.pdata.PWindowCollect;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.pdata.stream.Stream;
import com.antgroup.geaflow.pdata.stream.TransformType;
import java.util.Map;

public class WindowStreamCollect<T> extends Stream<T> implements PWindowCollect<T> {

    public WindowStreamCollect(Stream stream, AbstractOperator operator) {
        super(stream, operator);
    }

    @Override
    public WindowStreamCollect<T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public WindowStreamCollect<T> withName(String name) {
        setName(name);
        return this;
    }

    @Override
    public WindowStreamCollect<T> withConfig(Map map) {
        setConfig(map);
        return this;
    }

    @Override
    public WindowStreamCollect<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.StreamTransform;
    }
}
