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

package com.antgroup.geaflow.api.pdata.stream.window;

import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.PWindowCollect;
import com.antgroup.geaflow.api.pdata.stream.PStream;
import com.antgroup.geaflow.common.encoder.IEncoder;
import java.util.Map;

public interface PWindowStream<T> extends PStream<T> {

    /**
     * Transform T to R by mapFunction.
     */
    @Override
    <R> PWindowStream<R> map(MapFunction<T, R> mapFunction);

    /**
     * Filter T with filterFunction return false.
     */
    @Override
    PWindowStream<T> filter(FilterFunction<T> filterFunction);

    /**
     * Transform T into 0~n R by flatMapFunction.
     */
    @Override
    <R> PWindowStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction);

    /**
     * Perform union operation with uStream.
     */
    @Override
    PWindowStream<T> union(PStream<T> uStream);

    /**
     * Broadcast records to downstream.
     */
    PWindowBroadcastStream<T> broadcast();

    /**
     * Partition by some key based on selectorFunction.
     */
    @Override
    <KEY> PWindowKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction);

    /**
     * Output data by sinkFunction.
     */
    @Override
    PStreamSink<T> sink(SinkFunction<T> sinkFunction);

    /**
     * Collect result.
     */
    PWindowCollect<T> collect();

    /**
     * Set config.
     */
    @Override
    PWindowStream<T> withConfig(Map map);

    /**
     * Set config with key value pair.
     */
    @Override
    PWindowStream<T> withConfig(String key, String value);

    /**
     * Set name.
     */
    @Override
    PWindowStream<T> withName(String name);

    /**
     * Set parallelism of stream.
     */
    @Override
    PWindowStream<T> withParallelism(int parallelism);

    /**
     * Set encoder for performance.
     */
    @Override
    PWindowStream<T> withEncoder(IEncoder<T> encoder);

    /**
     * Returns the parallelism.
     */
    int getParallelism();
}
