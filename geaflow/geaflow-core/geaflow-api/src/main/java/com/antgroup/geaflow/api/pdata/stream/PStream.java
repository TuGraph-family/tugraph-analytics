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

package com.antgroup.geaflow.api.pdata.stream;

import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.base.PData;
import com.antgroup.geaflow.common.encoder.IEncoder;
import java.util.Map;

public interface PStream<T> extends PData {

    <R> PStream<R> map(MapFunction<T, R> mapFunction);

    PStream<T> filter(FilterFunction<T> filterFunction);

    <R> PStream<R> flatMap(FlatMapFunction<T, R> flatMapFunction);

    PStream<T> union(PStream<T> uStream);

    PStream<T> broadcast();

    <KEY> PKeyStream<KEY, T> keyBy(KeySelector<T, KEY> selectorFunction);

    PStreamSink<T> sink(SinkFunction<T> sinkFunction);

    @Override
    PStream<T> withConfig(Map map);

    @Override
    PStream<T> withConfig(String key, String value);

    @Override
    PStream<T> withName(String name);

    @Override
    PStream<T> withParallelism(int parallelism);


    PStream<T> withEncoder(IEncoder<T> encoder);

}
