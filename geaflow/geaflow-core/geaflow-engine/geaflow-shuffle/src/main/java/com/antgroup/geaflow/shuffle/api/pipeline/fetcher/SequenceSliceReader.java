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

package com.antgroup.geaflow.shuffle.api.pipeline.fetcher;

import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipeChannelBuffer;
import com.antgroup.geaflow.shuffle.api.pipeline.channel.ChannelId;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.network.netty.SliceOutputChannelHandler;
import java.io.IOException;

public class SequenceSliceReader implements PipelineSliceListener {

    private final ChannelId inputChannelId;
    private final SliceOutputChannelHandler requestHandler;

    private SliceId sliceId;
    private PipelineSliceReader sliceReader;
    private int sequenceNumber = -1;

    private volatile boolean isRegistered = false;
    private volatile boolean isReleased = false;

    public SequenceSliceReader(ChannelId inputChannelId, SliceOutputChannelHandler requestHandler) {
        this.inputChannelId = inputChannelId;
        this.requestHandler = requestHandler;
    }

    public void createSliceReader(SliceId sliceId, long startBatchId) throws IOException {
        this.sliceId = sliceId;
        this.sliceReader = ShuffleDataManager.getInstance().createSliceReader(sliceId,
            startBatchId, this);
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        requestHandler.notifyNonEmpty(this);
    }

    public void requestBatch(long batchId) {
        sliceReader.updateRequestedBatchId(batchId);
    }

    public boolean hasNext() {
        return sliceReader != null && sliceReader.hasNext();
    }

    public PipeChannelBuffer next() {
        if (isReleased) {
            throw new IllegalArgumentException("slice has been released already: " + sliceId);
        }
        PipeChannelBuffer next = sliceReader.next();
        if (next != null) {
            sequenceNumber++;
            return next;
        }
        return null;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public ChannelId getReceiverId() {
        return inputChannelId;
    }

    public void setRegistered(boolean registered) {
        this.isRegistered = registered;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    public void releaseAllResources() throws IOException {
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
        return "SequenceSliceReader{" + "sliceId=" + sliceId + '}';
    }

}
