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

package com.antgroup.geaflow.shuffle.api.writer;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.OutBuffer.BufferBuilder;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineShard;
import com.antgroup.geaflow.shuffle.api.pipeline.buffer.PipelineSlice;
import com.antgroup.geaflow.shuffle.memory.ShuffleDataManager;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.message.SliceId;
import com.antgroup.geaflow.shuffle.message.WriterId;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineShardBuffer<T, R> extends ShardBuffer<T, R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineWriter.class);

    private OutputFlusher outputFlusher;
    private final AtomicReference<Throwable> throwable;

    public PipelineShardBuffer() {
        this.throwable = new AtomicReference<>();
    }

    @Override
    public void init(IWriterContext writerContext) {
        super.init(writerContext);

        initResultSlices(targetChannels);

        String threadName = "OutputFlusher-" + Thread.currentThread().getName();
        int flushTimeout = this.shuffleConfig.getFlushBufferTimeoutMs();
        this.outputFlusher = new OutputFlusher(threadName, flushTimeout);
        this.outputFlusher.start();
    }

    private void initResultSlices(int channels) {
        PipelineSlice[] slices = new PipelineSlice[channels];
        WriterId writerID = new WriterId(pipelineId, edgeId, taskIndex);
        for (int i = 0; i < channels; i++) {
            slices[i] = new PipelineSlice(taskLogTag, new SliceId(writerID, i));
        }
        resultSlices = slices;
        ShuffleDataManager.getInstance().register(writerID, new PipelineShard(taskLogTag, slices));
    }

    @Override
    public void emit(long batchId, T value, boolean isRetract, int[] channels) throws IOException {
        checkError();
        super.emit(batchId, value, isRetract, channels);
    }

    @Override
    public Optional<R> finish(long batchId) throws IOException {
        checkError();
        for (int i = 0; i < buffers.size(); i++) {
            BufferBuilder bufferBuilder = buffers.get(i);
            if (bufferBuilder.getBufferSize() > 0) {
                send(i, bufferBuilder.build(), batchId);
            }
        }
        PipelineBarrier barrier = new PipelineBarrier(batchId, edgeId, taskIndex);
        notify(barrier);

        return Optional.empty();
    }

    public void flushAll() {
        PipelineSlice[] pipeSlices = resultSlices;
        boolean flushed = false;
        if (pipeSlices != null) {
            for (int i = 0; i < pipeSlices.length; i++) {
                if (null != pipeSlices[i]) {
                    pipeSlices[i].flush();
                    flushed = true;
                }
            }
        }
        if (!flushed) {
            LOGGER.warn("terminate flusher due to slices released");
            outputFlusher.terminate();
        }
    }

    @Override
    public void close() {
        if (outputFlusher != null) {
            outputFlusher.terminate();
        }
    }

    private void checkError() throws IOException {
        if (throwable.get() != null) {
            Throwable t = throwable.get();
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new GeaflowRuntimeException(t);
            }
        }
    }

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
            LOGGER.info("started {} with timeout:{}ms", name, timeout);
        }

        public void terminate() {
            if (running) {
                running = false;
                interrupt();
            }
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        // Propagate this if we are still running,
                        // because it should not happen in that case.
                        if (running) {
                            LOGGER.error("Interrupted", e);
                            throw e;
                        }
                    }

                    // Any errors here should let the thread come to a halt and be
                    // recognized by the writer.
                    flushAll();
                }
            } catch (Throwable t) {
                if (throwable.compareAndSet(null, t)) {
                    LOGGER.error("flush failed", t);
                }
            }
        }
    }

}
