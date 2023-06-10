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

package com.antgroup.geaflow.cluster.fetcher;

import com.antgroup.geaflow.cluster.task.runner.AbstractTaskRunner;
import com.antgroup.geaflow.common.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherRunner extends AbstractTaskRunner<IFetchRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FetcherRunner.class);

    private final PipelineInputFetcher fetcher;

    public FetcherRunner(Configuration configuration) {
        this.fetcher = new PipelineInputFetcher(configuration);
    }

    @Override
    protected void process(IFetchRequest task) {
        if (task instanceof InitFetchRequest) {
            fetcher.init((InitFetchRequest) task);
        } else {
            ReFetchRequest request = (ReFetchRequest) task;
            fetcher.fetch(request.getStartBatchId(), request.getWindowCount());
        }
    }

    @Override
    public void interrupt() {
        LOGGER.info("cancel fetcher runner");
        fetcher.cancel();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        fetcher.close();
    }

}
