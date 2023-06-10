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

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.common.encoder.EncoderResolver;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.operator.impl.io.WindowSourceOperator;
import com.antgroup.geaflow.pdata.stream.TransformType;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;
import java.util.Map;

public class WindowStreamSource<OUT> extends WindowDataStream<OUT> implements PWindowSource<OUT> {

    protected IWindow<OUT> windowFunction;

    public WindowStreamSource(IPipelineContext pipelineContext,
                              SourceFunction<OUT> sourceFunction,
                              IWindow<OUT> windowFunction) {
        super(pipelineContext, new WindowSourceOperator<>(sourceFunction, windowFunction));
        this.windowFunction = windowFunction;
        this.encoder = (IEncoder<OUT>) EncoderResolver.resolveFunction(SourceFunction.class, sourceFunction);
    }

    @Override
    public PWindowSource<OUT> build(IPipelineContext pipelineContext,
                                    SourceFunction<OUT> sourceFunction,
                                    IWindow<OUT> window) {
        return new WindowStreamSource<>(pipelineContext, sourceFunction, window);
    }

    @Override
    public WindowStreamSource<OUT> window(IWindow<OUT> window) {
        this.windowFunction = window;
        return this;
    }


    @Override
    public WindowStreamSource<OUT> withConfig(Map map) {
        super.withConfig(map);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withConfig(String key, String value) {
        super.withConfig(key, value);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withName(String name) {
        super.withName(name);
        return this;
    }

    @Override
    public WindowStreamSource<OUT> withParallelism(int parallelism) {
        super.withParallelism(parallelism);
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.StreamSource;
    }

    @Override
    public WindowStreamSource<OUT> withEncoder(IEncoder<OUT> encoder) {
        this.encoder = encoder;
        return this;
    }

}
