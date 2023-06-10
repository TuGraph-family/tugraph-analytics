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

package com.antgroup.geaflow.shuffle.api.reader;

import com.antgroup.geaflow.common.metric.ShuffleReadMetrics;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineEvent;

public interface IShuffleFetcher<T extends ISliceMeta> {

    /**
     * Init shuffle fetcher with fetch context.
     *
     * @param fetchContext fetch context.
     */
    void init(FetchContext<T> fetchContext);

    /**
     * If more data.
     *
     * @return true if there is more data, otherwise false.
     */
    boolean hasNext();

    /**
     * Get the next pipeline event.
     *
     * @return next pipeline event.
     */
    PipelineEvent next();

    /**
     * Get the shuffle metrics.
     *
     * @return shuffle metrics.
     */
    ShuffleReadMetrics getReadMetrics();

    /**
     * Close.
     */
    void close();

}
