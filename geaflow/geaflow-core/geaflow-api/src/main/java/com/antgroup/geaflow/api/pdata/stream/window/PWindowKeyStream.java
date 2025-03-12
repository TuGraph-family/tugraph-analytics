/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.api.pdata.stream.window;

import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.api.pdata.stream.PKeyStream;
import com.antgroup.geaflow.api.pdata.stream.view.PIncStreamView;
import com.antgroup.geaflow.common.encoder.IEncoder;
import java.util.Map;

public interface PWindowKeyStream<KEY, T> extends PWindowStream<T>, PKeyStream<KEY, T> {

    /**
     * Default is incremental reduce compute, otherwise is window reduce compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    @Override
    PWindowStream<T> reduce(ReduceFunction<T> reduceFunction);

    /**
     * Default is incremental aggregate compute, otherwise is window aggregate compute if INC_STREAM_MATERIALIZE_DISABLE is true.
     */
    @Override
    <ACC, OUT> PWindowStream<OUT> aggregate(AggregateFunction<T, ACC, OUT> aggregateFunction);

    /**
     * Build incremental stream view.
     */
    PIncStreamView<T> materialize();

    @Override
    PWindowKeyStream<KEY, T> withConfig(Map config);

    @Override
    PWindowKeyStream<KEY, T> withConfig(String key, String value);

    @Override
    PWindowKeyStream<KEY, T> withName(String name);

    @Override
    PWindowKeyStream<KEY, T> withParallelism(int parallelism);

    /**
     * Set the encoder for performance.
     */
    @Override
    PWindowKeyStream<KEY, T> withEncoder(IEncoder<T> encoder);


}
