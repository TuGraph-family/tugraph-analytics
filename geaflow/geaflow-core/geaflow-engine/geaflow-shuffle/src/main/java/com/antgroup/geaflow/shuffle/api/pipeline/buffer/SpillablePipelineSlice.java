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

package com.antgroup.geaflow.shuffle.api.pipeline.buffer;

import com.antgroup.geaflow.common.encoder.Encoders;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import com.antgroup.geaflow.shuffle.memory.ShuffleMemoryTracker;
import com.antgroup.geaflow.shuffle.message.SliceId;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpillablePipelineSlice extends PipelineSlice {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpillablePipelineSlice.class);

    private static final int BUFFER_SIZE = 64 * 1024;

    private final String fileName;
    private List<PipeBuffer> buffers = new ArrayList<>();
    private Iterator<PipeBuffer> iterator;
    private OutputStream outputStream;
    private InputStream inputStream;
    private PipeBuffer value;
    private volatile boolean ready2read = false;

    // Whether force write data to memory;
    private final boolean forceMemory;
    // Whether force write data to disk;
    private final boolean forceDisk;
    // Bytes count in memory.
    private long memoryBytes = 0;
    // Bytes count on disk.
    private long diskBytes = 0;

    public SpillablePipelineSlice(String taskLogTag, SliceId sliceId, int refCount) {
        super(taskLogTag, sliceId, refCount);
        this.fileName = String.format("shuffle-%d-%d-%d",
            sliceId.getPipelineId(), sliceId.getEdgeId(), sliceId.getSliceIndex());
        this.forceMemory = ShuffleConfig.getInstance().isForceMemory();
        this.forceDisk = ShuffleConfig.getInstance().isForceDisk();
    }

    public String getFileName() {
        return this.fileName;
    }

    //////////////////////////////
    // Produce data.
    //////////////////////////////

    @Override
    public boolean add(PipeBuffer buffer) {
        if (this.isReleased || this.ready2read) {
            throw new GeaflowRuntimeException("slice already released or mark finish: " + this.getSliceId());
        }
        if (this.forceMemory) {
            this.writeMemory(buffer);
            return true;
        }
        if (this.forceDisk) {
            this.writeDisk(buffer);
            return true;
        }

        boolean enoughMemory = ShuffleMemoryTracker.getInstance().requireMemory(1);
        if (enoughMemory) {
            this.writeMemory(buffer);
        } else {
            this.writeDisk(buffer);
        }
        return true;
    }

    private void writeMemory(PipeBuffer buffer) {
        this.buffers.add(buffer);
        this.memoryBytes += buffer.getBufferSize();
    }

    private void writeDisk(PipeBuffer buffer) {
        try {
            this.write2disk(buffer);
            this.diskBytes += buffer.getBufferSize();
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private void write2disk(PipeBuffer buffer) throws IOException {
        if (this.outputStream == null) {
            this.outputStream = this.createTmpFile();
        }
        OutBuffer outBuffer = buffer.getBuffer();
        if (outBuffer == null) {
            Encoders.INTEGER.encode(0, this.outputStream);
            Encoders.LONG.encode(buffer.getBatchId(), this.outputStream);
            Encoders.INTEGER.encode(buffer.getCount(), this.outputStream);
        } else {
            Encoders.INTEGER.encode(outBuffer.getBufferSize(), this.outputStream);
            Encoders.LONG.encode(buffer.getBatchId(), this.outputStream);
            buffer.getBuffer().write(this.outputStream);
        }

    }

    private OutputStream createTmpFile() {
        try {
            Path path = Paths.get(this.fileName);
            Files.deleteIfExists(path);
            Files.createFile(path);
            return new BufferedOutputStream(Files.newOutputStream(path, StandardOpenOption.WRITE), BUFFER_SIZE);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    private InputStream readFromTmpFile() {
        try {
            Path path = Paths.get(this.fileName);
            return new BufferedInputStream(Files.newInputStream(path, StandardOpenOption.READ), BUFFER_SIZE);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public void flush() {
        this.iterator = this.buffers.iterator();
        this.ready2read = true;
        if (this.outputStream != null) {
            try {
                this.outputStream.flush();
                this.outputStream.close();
                this.outputStream = null;
                this.inputStream = this.readFromTmpFile();
            } catch (IOException e) {
                throw new GeaflowRuntimeException(e);
            }
        }
        LOGGER.info("write file {} {} {}", this.fileName, this.memoryBytes, this.diskBytes);
    }


    //////////////////////////////
    // Consume data.
    //////////////////////////////

    @Override
    public PipeBuffer next() {
        PipeBuffer next = this.value;
        this.value = null;
        return next;
    }

    @Override
    public boolean hasNext() {
        if (this.iterator.hasNext()) {
            this.value = this.iterator.next();
            return true;
        }

        try {
            if (this.inputStream == null || this.inputStream.available() <= 0) {
                return false;
            }
            int size = Encoders.INTEGER.decode(this.inputStream);
            long batchId = Encoders.LONG.decode(this.inputStream);
            if (size == 0) {
                int count = Encoders.INTEGER.decode(this.inputStream);
                this.value = new PipeBuffer(batchId, count, false, true);
            } else {
                byte[] bytes = new byte[size];
                int read = this.inputStream.read(bytes);
                if (read != bytes.length) {
                    String msg = String.format("illegal read size, expect %d, actual %d", bytes.length, read);
                    throw new GeaflowRuntimeException(msg);
                }
                this.value = new PipeBuffer(bytes, batchId, true);
            }
            return true;
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public boolean isReady2read() {
        return this.ready2read;
    }

    @Override
    public synchronized void release() {
        if (this.isReleased) {
            return;
        }
        this.buffers.clear();
        this.buffers = null;
        try {
            Path path = Paths.get(this.fileName);
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new GeaflowRuntimeException(e);
        }
        this.isReleased = true;
    }

}
