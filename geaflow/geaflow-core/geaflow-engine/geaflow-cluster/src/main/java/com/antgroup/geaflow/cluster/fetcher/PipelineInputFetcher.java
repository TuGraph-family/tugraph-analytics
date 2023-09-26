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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.metric.EventMetrics;
import com.antgroup.geaflow.common.metric.ShuffleReadMetrics;
import com.antgroup.geaflow.io.AbstractMessageBuffer;
import com.antgroup.geaflow.shuffle.api.reader.IShuffleReader;
import com.antgroup.geaflow.shuffle.message.FetchRequest;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.message.PipelineEvent;
import com.antgroup.geaflow.shuffle.message.PipelineMessage;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineInputFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineInputFetcher.class);

    private Configuration config;
    private IShuffleReader shuffleReader;
    private InitFetchRequest initRequest;

    private List<IInputMessageBuffer<?>> fetchListeners;
    private BarrierHandler barrierHandler;

    private long pipelineId;
    private String pipelineName;

    public PipelineInputFetcher(Configuration config) {
        this.config = config;
    }

    /**
     * Init input fetcher reader.
     *
     * @param request
     */
    public void init(InitFetchRequest request) {
        // Close the previous reader.
        if (this.shuffleReader != null) {
            this.shuffleReader.close();
            this.shuffleReader = null;
        }
        // Load shuffle reader according to shuffle type.
        this.shuffleReader = ShuffleManager.getInstance().loadShuffleReader(config);
        this.pipelineId = request.getPipelineId();
        this.pipelineName = request.getPipelineName();
        this.fetchListeners = request.getFetchListeners();
        this.barrierHandler = new BarrierHandler(request.getTaskId(), request.getTotalSliceNum());
        this.initRequest = request;
        for (IEncoder<?> encoder : this.initRequest.getEncoders().values()) {
            if (encoder != null) {
                encoder.init(this.config);
            }
        }
    }

    /**
     * Fetch batch data and trigger process.
     */
    public void fetch(long startWindowId, long windowCount) {
        LOGGER.info("task {} start fetch windowId:{} count:{}", initRequest.getTaskId(),
            startWindowId, windowCount);
        long targetWindowId = startWindowId + windowCount - 1;
        fetch(buildFetchRequest(targetWindowId));
    }

    public void cancel() {
        // TODO Cancel fetching task.
        //      Shuffle reader should support cancel.
    }

    /**
     * Close the shuffle reader if need.
     */
    public void close() {
        try {
            if (shuffleReader != null) {
                shuffleReader.close();
            }
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            throw new GeaflowRuntimeException(t);
        }
    }

    /**
     * Build the fetch request.
     * @return
     */
    protected FetchRequest buildFetchRequest(long targetBatchId) {
        FetchRequest request = new FetchRequest();
        request.setPipelineId(pipelineId);
        request.setPipelineName(pipelineName);
        request.setTaskId(initRequest.getTaskId());
        request.setTaskIndex(initRequest.getTaskIndex());
        request.setTaskName(initRequest.getTaskName());
        request.setVertexId(initRequest.getVertexId());
        request.setTargetBatchId(targetBatchId);
        request.setInputStreamMap(initRequest.getInputStreamMap());
        request.setDescriptor(initRequest.getDescriptor());
        request.setInputSlices(initRequest.getInputSlices());
        request.setEncoders(initRequest.getEncoders());
        return request;
    }

    /**
     * Fetch data according to fetch request and process by worker.
     *
     * @param request
     */
    protected void fetch(FetchRequest request) {
        shuffleReader.fetch(request);
        try {
            while (shuffleReader.hasNext()) {
                PipelineEvent event = shuffleReader.next();
                if (event != null) {
                    if (event instanceof PipelineMessage) {
                        PipelineMessage message = (PipelineMessage) event;
                        for (IInputMessageBuffer<?> listener : fetchListeners) {
                            listener.onMessage(message);
                        }
                    } else {
                        PipelineBarrier barrier = (PipelineBarrier) event;
                        if (barrierHandler.checkCompleted(barrier)) {
                            long windowId = barrier.getWindowId();
                            long windowCount = barrierHandler.getTotalWindowCount();
                            this.handleMetrics();
                            for (IInputMessageBuffer<?> listener : fetchListeners) {
                                listener.onBarrier(windowId, windowCount);
                            }
                        }
                    }
                }
            }
            LOGGER.info("task {} worker reader finish fetch windowId {}",
                    request.getTaskId(), request.getTargetBatchId());
        } catch (Throwable e) {
            LOGGER.error("fetcher encounters unexpected exception: {}", e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    private void handleMetrics() {
        ShuffleReadMetrics shuffleReadMetrics = this.shuffleReader.getShuffleReadMetrics();
        for (IInputMessageBuffer<?> listener : this.fetchListeners) {
            EventMetrics eventMetrics = ((AbstractMessageBuffer<?>) listener).getEventMetrics();
            eventMetrics.addShuffleReadBytes(shuffleReadMetrics.getDecodeBytes());
        }
    }

}
