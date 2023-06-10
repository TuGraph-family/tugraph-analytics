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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import java.util.ServiceLoader;

public class ShuffleFetcherFactory {

    public static IShuffleFetcher getShuffleFetcher(DataExchangeMode exchangeMode,
                                                    IConnectionManager connectionManager, Configuration config) {
        return exchangeMode == DataExchangeMode.PIPELINE
            ? getPipelineFetcher(connectionManager, config)
            : getHybridFetcher(connectionManager, config);
    }

    public static PipelineFetcher getPipelineFetcher(IConnectionManager connectionManager, Configuration config) {
        PipelineFetcher pipelineFetcher = new PipelineFetcher();
        pipelineFetcher.setup(connectionManager, config);
        return pipelineFetcher;
    }

    public static HybridFetcher getHybridFetcher(IConnectionManager connectionManager, Configuration config) {
        for (HybridFetcher hybridFetcher : ServiceLoader.load(HybridFetcher.class)) {
            hybridFetcher.setup(connectionManager, config);
            return hybridFetcher;
        }
        throw new GeaflowRuntimeException("no hybrid fetcher impl found");
    }

}
