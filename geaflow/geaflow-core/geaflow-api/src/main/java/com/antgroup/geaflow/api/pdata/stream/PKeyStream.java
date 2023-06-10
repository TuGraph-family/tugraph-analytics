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

import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.common.encoder.IEncoder;
import java.util.Map;

public interface PKeyStream<KEY, T> extends PStream<T> {

    /**
     * Default is incremental reduce compute, otherwise is window reduce compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    PStream<T> reduce(ReduceFunction<T> reduceFunction);

    /**
     * Default is incremental aggregate compute, otherwise is window aggregate compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    <ACC, OUT> PStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction);

    @Override
    PKeyStream<KEY, T> withConfig(Map map);

    @Override
    PKeyStream<KEY, T> withConfig(String key, String value);

    @Override
    PKeyStream<KEY, T> withName(String name);

    @Override
    PKeyStream<KEY, T> withParallelism(int parallelism);

    @Override
    PKeyStream<KEY, T> withEncoder(IEncoder<T> encoder);

}
