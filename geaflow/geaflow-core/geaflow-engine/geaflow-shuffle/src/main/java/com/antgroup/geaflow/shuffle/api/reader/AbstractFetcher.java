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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SHUFFLE_MAX_BYTES_IN_FLIGHT;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.metric.ShuffleReadMetrics;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.message.FetchRequest;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.serialize.AbstractMessageIterator;
import com.antgroup.geaflow.shuffle.serialize.EncoderMessageIterator;
import com.antgroup.geaflow.shuffle.serialize.IMessageIterator;
import com.antgroup.geaflow.shuffle.serialize.MessageIterator;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFetcher<T extends ISliceMeta> implements IShuffleFetcher<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFetcher.class);

    protected int processedNum;
    protected int totalSliceNum;
    protected String streamName;
    protected long targetBatchId;

    protected String taskName;
    protected int taskIndex;
    protected ShuffleReadMetrics readMetrics;

    protected long maxBytesInFlight;
    protected ShuffleConfig shuffleConfig;
    protected Map<Integer, IEncoder<?>> encoders;

    public AbstractFetcher() {
    }

    public void setup(IConnectionManager connectionManager, Configuration config) {
        this.maxBytesInFlight = config.getLong(SHUFFLE_MAX_BYTES_IN_FLIGHT);
        this.shuffleConfig = ShuffleConfig.getInstance();
        this.totalSliceNum = 0;
    }

    @Override
    public void init(FetchContext<T> fetchContext) {
        FetchRequest req = fetchContext.getRequest();
        this.targetBatchId = req.getTargetBatchId();
        this.taskIndex = req.getTaskIndex();
        this.taskName = req.getTaskName();
        this.readMetrics = new ShuffleReadMetrics();
        this.processedNum = 0;
        this.encoders = req.getEncoders() == null ? new HashMap<>() : req.getEncoders();
    }

    @Override
    public boolean hasNext() {
        return processedNum < totalSliceNum;
    }

    public ShuffleReadMetrics getReadMetrics() {
        return readMetrics;
    }

    @Override
    public void close() {
    }

    protected IMessageIterator<?> getMessageIterator(int edgeId, OutBuffer outBuffer) {
        IEncoder<?> encoder = this.encoders.get(edgeId);
        AbstractMessageIterator<?> messageIterator = encoder == null
            ? new MessageIterator<>(outBuffer)
            : new EncoderMessageIterator<>(outBuffer, encoder);
        return messageIterator;
    }

    protected IMessageIterator<?> getMessageIterator(int edgeId, InputStream inputStream) {
        IEncoder<?> encoder = this.encoders.get(edgeId);
        AbstractMessageIterator<?> messageIterator = encoder == null
            ? new MessageIterator<>(inputStream)
            : new EncoderMessageIterator<>(inputStream, encoder);
        return messageIterator;
    }

}
