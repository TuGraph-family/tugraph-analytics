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
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.shuffle.message.FetchRequest;
import com.antgroup.geaflow.shuffle.message.PipelineEvent;
import java.io.Serializable;

public interface IShuffleReader extends Serializable {

    /**
     * Init shuffle reader with reader context.
     *
     * @param context reader context.
     */
    void init(IReaderContext context);

    /**
     * Fetch upstream shards.
     *
     * @param req description of fetched batches.
     */
    void fetch(FetchRequest req);

    /**
     * Returns true if the requested batches is not fetched completely.
     *
     * @return true/false.
     */
    boolean hasNext();

    /**
     * Returns the next batch.
     * @return batch data or event.
     */
    PipelineEvent next();

    /**
     * Get the exchange mode.
     *
     * @return exchange mode.
     */
    DataExchangeMode getExchangeMode();

    /**
     * Get read metrics.
     *
     * @return read metrics.
     */
    ShuffleReadMetrics getShuffleReadMetrics();

    /**
     * Close.
     */
    void close();

}
