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

import com.antgroup.geaflow.api.function.base.FilterFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;

// import com.antgroup.geaflow.api.function.base.JoinFunction;

import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;

// import com.antgroup.geaflow.api.function.internal.IKeySelector;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.PWindowCollect;
import com.antgroup.geaflow.api.pdata.stream.PStream;
import com.antgroup.geaflow.common.encoder.IEncoder;

// import com.antgroup.geaflow.dsl.rel.match.IMatchNode;

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

    // PWindowStream<T> optionalMatch(IMatchNode pathPattern, boolean isCaseSensitive);

    // /**
    // * Performs an optional match operation with another stream of Paths.
    // * This operation is designed to work with streams of {@link Path} objects,
    // * using KeySelectors to extract join keys and a {@link StepJoinFunction}
    // * to merge matching paths.
    // *
    // * @param other the other PWindowStream of Paths to join with.
    // * @param leftKeySelector Key selector for this stream (must be a stream of
    // * Paths).
    // * @param rightKeySelector Key selector for the other stream.
    // * @param joinFunction The function to combine the matched Path records.
    // * @param <K> The type of the key to join on.
    // * @return A new PWindowStream containing the result of the optional match,
    // * which are also Paths.
    // */
    // <K> PWindowStream<Path> optionalMatch(PWindowStream<Path> other,
    // KeySelector<Path, K> leftKeySelector,
    // KeySelector<Path, K> rightKeySelector,
    // StepJoinFunction joinFunction);

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
