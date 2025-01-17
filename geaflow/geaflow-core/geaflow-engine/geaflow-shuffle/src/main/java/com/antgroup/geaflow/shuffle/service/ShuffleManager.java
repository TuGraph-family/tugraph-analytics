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

package com.antgroup.geaflow.shuffle.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.api.reader.IShuffleReader;
import com.antgroup.geaflow.shuffle.api.writer.IShuffleWriter;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.message.Shard;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.network.netty.ConnectionManager;
import com.antgroup.geaflow.shuffle.pipeline.buffer.ShuffleMemoryTracker;
import com.antgroup.geaflow.shuffle.pipeline.slice.SliceManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleManager.class);

    private static ShuffleManager INSTANCE;
    private final IShuffleService shuffleService;
    private final ConnectionManager connectionManager;
    private final SliceManager sliceManager;
    private final ShuffleConfig shuffleConfig;
    private final ShuffleMemoryTracker shuffleMemoryTracker;

    public ShuffleManager(Configuration config) {
        this.shuffleConfig = new ShuffleConfig(config);
        this.connectionManager = new ConnectionManager(shuffleConfig);
        this.shuffleService = new NettyShuffleService();
        this.shuffleService.init(connectionManager);
        this.sliceManager = new SliceManager();
        this.shuffleMemoryTracker = new ShuffleMemoryTracker(config);
    }

    public static synchronized ShuffleManager init(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new ShuffleManager(config);
        }
        return INSTANCE;
    }

    public static ShuffleManager getInstance() {
        return INSTANCE;
    }

    public IConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public SliceManager getSliceManager() {
        return sliceManager;
    }

    public ShuffleConfig getShuffleConfig() {
        return shuffleConfig;
    }

    public ShuffleMemoryTracker getShuffleMemoryTracker() {
        return shuffleMemoryTracker;
    }

    public int getShufflePort() {
        return connectionManager.getShuffleAddress().port();
    }

    public IShuffleReader loadShuffleReader() {
        return shuffleService.getReader();
    }

    public IShuffleWriter<?, Shard> loadShuffleWriter() {
        return shuffleService.getWriter();
    }

    public void release(long pipelineId) {
        sliceManager.release(pipelineId);
    }

    public void close() {
        LOGGER.info("closing shuffle manager");
        try {
            connectionManager.close();
            shuffleMemoryTracker.release();
            INSTANCE = null;
        } catch (IOException e) {
            LOGGER.warn("close connectManager failed:{}", e.getCause(), e);
        }
    }
}
