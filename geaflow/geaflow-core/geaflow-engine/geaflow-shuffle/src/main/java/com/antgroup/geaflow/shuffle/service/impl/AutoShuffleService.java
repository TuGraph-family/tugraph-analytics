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

package com.antgroup.geaflow.shuffle.service.impl;

import com.antgroup.geaflow.shuffle.api.reader.IShuffleReader;
import com.antgroup.geaflow.shuffle.api.reader.PipelineReader;
import com.antgroup.geaflow.shuffle.api.writer.IShuffleWriter;
import com.antgroup.geaflow.shuffle.api.writer.PipelineWriter;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import com.antgroup.geaflow.shuffle.service.IShuffleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoShuffleService implements IShuffleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoShuffleService.class);

    private IConnectionManager connectionManager;

    @Override
    public void init(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public IShuffleReader getReader() {
        return new PipelineReader(connectionManager);
    }

    @Override
    public IShuffleWriter getWriter() {
        return new PipelineWriter(connectionManager);
    }

    @Override
    public void clean(PipelineInfo jobInfo) {
        LOGGER.info("release shuffle data of job {}", jobInfo);
        ShuffleDataManager.getInstance().release(jobInfo.getPipelineId());
    }
}
