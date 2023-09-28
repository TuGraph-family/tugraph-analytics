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

package com.antgroup.geaflow.shuffle.api.pipeline.channel;

import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeChannelBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.OneShardFetcher;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceListener;
import com.antgroup.geaflow.shuffle.api.pipeline.fetcher.PipelineSliceReader;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.util.SliceNotFoundException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel.
 */
public class LocalInputChannel extends AbstractInputChannel implements PipelineSliceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalInputChannel.class);

    private final Object requestLock = new Object();
    private PipelineSliceReader sliceReader;
    private volatile boolean isReleased;

    public LocalInputChannel(
        OneShardFetcher fetcher,
        SliceId inputSlice,
        int channelIndex,
        int initialBackoff,
        int maxBackoff,
        long startBatchId) {
        super(channelIndex, fetcher, inputSlice, initialBackoff, maxBackoff, startBatchId);
    }

    @Override
    public void requestSlice(long batchId) throws IOException {
        boolean retriggerRequest = false;
        boolean notifyAvailable = false;

        // The lock is required to request only once in the presence of retriggered requests.
        synchronized (requestLock) {
            Preconditions.checkState(!isReleased, "LocalInputChannel has been released already");
            if (sliceReader == null) {
                LOGGER.info("Requesting Local slice {}", inputSliceId);
                try {
                    sliceReader = ShuffleDataManager.getInstance().createSliceReader(inputSliceId
                        , initialBatchId, this);
                    notifyAvailable = true;
                } catch (SliceNotFoundException notFound) {
                    if (increaseBackoff()) {
                        retriggerRequest = true;
                    } else {
                        LOGGER.warn("not found slice:{}", inputSliceId);
                        throw notFound;
                    }
                }
            } else {
                sliceReader.updateRequestedBatchId(batchId);
                if (sliceReader.hasNext()) {
                    notifyDataAvailable();
                }
            }
        }

        if (notifyAvailable) {
            notifyDataAvailable();
        }
        // Do this outside of the lock scope as this might lead to a
        // deadlock with a concurrent release of the channel via the
        // input fetcher.
        if (retriggerRequest) {
            inputFetcher.retriggerFetchRequest(inputSliceId);
        }
    }

    public void retriggerSliceRequest(Timer timer) {
        synchronized (requestLock) {
            Preconditions.checkState(sliceReader == null, "already requested slice");
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        requestSlice(initialBatchId);
                    } catch (Throwable t) {
                        setError(t);
                    }
                }
            }, getCurrentBackoff());
        }
    }

    @Override
    public Optional<PipeChannelBuffer> getNext() throws IOException {
        checkError();
        PipelineSliceReader reader = this.sliceReader;
        if (reader == null) {
            if (isReleased) {
                return Optional.empty();
            }
            reader = checkAndGetSliceReader();
        }

        PipeChannelBuffer next = reader.next();

        if (next == null) {
            return Optional.empty();
        }

        next.setSliceId(inputSliceId);
        return Optional.of(next);
    }

    @Override
    public void notifyDataAvailable() {
        notifyChannelNonEmpty();
    }

    private PipelineSliceReader checkAndGetSliceReader() {
        // Synchronizing on the request lock means this blocks until the asynchronous request
        // for the slice has been completed by then the slice reader is visible or the channel is released.
        synchronized (requestLock) {
            Preconditions.checkState(!isReleased, "released");
            Preconditions.checkState(sliceReader != null, "reader is not ready.");
            return sliceReader;
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public void release() {
        if (!isReleased) {
            isReleased = true;
            PipelineSliceReader reader = sliceReader;
            if (reader != null) {
                reader.release();
                sliceReader = null;
            }
        }
    }

    @Override
    public String toString() {
        return "LocalInputChannel [" + inputSliceId + "]";
    }

}
