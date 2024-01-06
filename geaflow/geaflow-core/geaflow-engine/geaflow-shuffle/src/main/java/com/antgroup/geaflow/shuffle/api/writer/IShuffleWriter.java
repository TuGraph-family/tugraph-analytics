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

package com.antgroup.geaflow.shuffle.api.writer;

import com.antgroup.geaflow.common.metric.ShuffleWriteMetrics;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface IShuffleWriter<T, R>  {

    /**
     * Init with shuffle writer context.
     *
     * @param writerContext writer context.
     */
    void init(IWriterContext writerContext);

    /**
     * Emit value to output channels.
     *
     * @param channels output channels.
     * @throws IOException io exception.
     */
    void emit(long batchId, T value, boolean isRetract, int[] channels) throws IOException;

    /**
     * Emit values to output channels.
     *
     * @param batchId batch id
     * @param value data list
     * @param isRetract if retract
     * @param channels output channels
     * @throws IOException err
     */
    void emit(long batchId, List<T> value, boolean isRetract, int channels) throws IOException;

    /**
     * Flush buffered data.
     *
     * @return shuffle result.
     * @throws IOException io exception.
     */
    Optional<R> flush(long batchId) throws IOException;

    /**
     * Get write metrics.
     *
     * @return write metrics.
     */
    ShuffleWriteMetrics getShuffleWriteMetrics();

    /**
     * Close.
     */
    void close();
}
