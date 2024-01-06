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
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineReader implements IShuffleReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineReader.class);

    private final IConnectionManager connectionManager;
    private IReaderContext readerContext;
    private IShuffleFetcher shuffleFetcher;

    public PipelineReader(IConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void init(IReaderContext context) {
        this.readerContext = context;
    }

    @Override
    public void fetch(FetchRequest req) {
        DataExchangeMode exchangeMode = req.getDescriptor().getExchangeMode();
        initShuffleFetcher(exchangeMode);
        shuffleFetcher.init(new FetchContext<>(req));
    }

    private void initShuffleFetcher(DataExchangeMode exchangeMode) {
        if (shuffleFetcher == null) {
            this.shuffleFetcher = ShuffleFetcherFactory.getShuffleFetcher(
                exchangeMode, this.connectionManager, this.readerContext.getConfig());
        }
    }

    @Override
    public boolean hasNext() {
        return shuffleFetcher.hasNext();
    }

    @Override
    public PipelineEvent next() {
        return shuffleFetcher.next();
    }

    @Override
    public ShuffleReadMetrics getShuffleReadMetrics() {
        return this.shuffleFetcher.getReadMetrics();
    }

    @Override
    public void close() {
        try {
            if (shuffleFetcher != null) {
                shuffleFetcher.close();
            }
        } catch (Exception e) {
            LOGGER.warn("close reader failed", e);
        }
    }

    @Override
    public DataExchangeMode getExchangeMode() {
        return DataExchangeMode.PIPELINE;
    }

}
