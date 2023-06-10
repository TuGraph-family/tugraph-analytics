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

import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.pdata.stream.Stream;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import java.util.Map;

public class WindowStreamSink<T> extends Stream<T> implements PStreamSink<T> {

    public WindowStreamSink(IPipelineContext pipelineContext) {
        super(pipelineContext);
    }

    public WindowStreamSink(IPipelineContext pipelineContext, AbstractOperator operator) {
        super(pipelineContext, operator);
    }

    public WindowStreamSink(Stream stream,
                            AbstractOperator operator) {
        super(stream, operator);
    }


    @Override
    public WindowStreamSink<T> withConfig(Map map) {
        setConfig(map);
        return this;
    }

    @Override
    public WindowStreamSink<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public WindowStreamSink<T> withName(String name) {
        setName(name);
        return this;
    }

    public WindowStreamSink<T> withParallelism(int parallelism) {
        super.setParallelism(parallelism);
        return this;
    }

}
