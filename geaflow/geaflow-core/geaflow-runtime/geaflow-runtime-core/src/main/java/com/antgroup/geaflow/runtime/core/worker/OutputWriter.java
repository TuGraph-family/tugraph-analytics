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

package com.antgroup.geaflow.runtime.core.worker;

import com.antgroup.geaflow.cluster.collector.IOutputMessageBuffer;
import com.antgroup.geaflow.cluster.protocol.OutputMessage;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.io.AbstractMessageBuffer;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.message.Shard;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class OutputWriter<T> extends AbstractMessageBuffer<OutputMessage<T>>
    implements IOutputMessageBuffer<T, Shard> {

    private CompletableFuture<Shard> resultFuture = new CompletableFuture<>();

    private final int edgeId;
    private final int bufferSize;
    private final ArrayList<T>[] buffers;
    private final AtomicReference<Throwable> err;

    public OutputWriter(int edgeId, int bucketNum) {
        super(ShuffleConfig.getInstance().getEmitQueueSize());
        this.edgeId = edgeId;
        this.bufferSize = ShuffleConfig.getInstance().getEmitBufferSize();
        this.buffers = new ArrayList[bucketNum];
        this.err = new AtomicReference<>();
        for (int i = 0; i < bucketNum; i++) {
            this.buffers[i] = new ArrayList<>(this.bufferSize);
        }
    }

    @Override
    public void emit(long windowId, T data, boolean isRetract, int[] targetChannels) {
        for (int channel : targetChannels) {
            ArrayList<T> buffer = this.buffers[channel];
            buffer.add(data);
            if (buffer.size() == this.bufferSize) {
                this.checkErr();
                long start = System.currentTimeMillis();
                this.offer(OutputMessage.data(windowId, channel, buffer));
                this.eventMetrics.addShuffleWriteCostMs(System.currentTimeMillis() - start);
                this.buffers[channel] = new ArrayList<>(this.bufferSize);
            }
        }
    }

    @Override
    public void setResult(long windowId, Shard result) {
        this.resultFuture.complete(result);
    }

    @Override
    public Shard finish(long windowId) {
        this.checkErr();
        long start = System.currentTimeMillis();
        for (int i = 0; i < this.buffers.length; i++) {
            ArrayList<T> buffer = this.buffers[i];
            if (!buffer.isEmpty()) {
                this.offer(OutputMessage.data(windowId, i, buffer));
                this.buffers[i] = new ArrayList<>(this.bufferSize);
            }
        }

        this.offer(OutputMessage.barrier(windowId));
        try {
            Shard shard = this.resultFuture.get();
            this.resultFuture = new CompletableFuture<>();
            this.eventMetrics.addShuffleWriteCostMs(System.currentTimeMillis() - start);
            return shard;
        } catch (InterruptedException | ExecutionException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void error(Throwable t) {
        if (this.err.get() == null) {
            this.err.set(t);
        }
    }

    public void checkErr() {
        if (this.err.get() != null) {
            throw new GeaflowRuntimeException(this.err.get());
        }
    }

    public int getEdgeId() {
        return this.edgeId;
    }

}
