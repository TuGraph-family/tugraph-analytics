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

package com.antgroup.geaflow.api.graph.function.aggregate;

/**
 * Interface for vertex centric aggregate context function.
 * @param <I> The aggregate iterm.
 * @param <R> The aggregate result.
 */
public interface VertexCentricAggContextFunction<I, R> {
    /**
     * Init aggregation context.
     */
    void initContext(VertexCentricAggContext<I, R> aggContext);

    interface VertexCentricAggContext<I, R> {

        /**
         * Return current global aggregation result.
         */
        R getAggregateResult();

        /**
         * Do aggregate for input iterm.
         */
        void aggregate(I iterm);

    }
}
