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
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class PipelineWriter<T, R> implements IShuffleWriter<T, R> {

    private ShardBuffer shardBuffer;
    private final IConnectionManager connectionManager;

    public PipelineWriter(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void init(IWriterContext writerContext) {
        ShuffleDescriptor descriptor = writerContext.getShuffleDescriptor();
        shardBuffer = ShardBufferFactory
            .getShardBuffer(descriptor.getExchangeMode(), connectionManager);
        shardBuffer.init(writerContext);
    }

    @Override
    public void emit(long batchId, T value, boolean isRetract, int[] channels) throws IOException {
        shardBuffer.emit(batchId, value, isRetract, channels);
    }

    @Override
    public void emit(long batchId, List<T> data, boolean isRetract, int channel) throws IOException {
        this.shardBuffer.emit(batchId, data, channel);
    }

    @Override
    public Optional<R> flush(long batchId) throws IOException {
        return shardBuffer.finish(batchId);
    }

    @Override
    public ShuffleWriteMetrics getShuffleWriteMetrics() {
        return this.shardBuffer.getShuffleWriteMetrics();
    }

    @Override
    public void close() {
        if (shardBuffer != null) {
            shardBuffer.close();
        }
    }

}
