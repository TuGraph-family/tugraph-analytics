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

package com.antgroup.geaflow.shuffle.pipeline.buffer;

import io.netty.channel.FileRegion;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface OutBuffer {

    /**
     * Get the input stream of this buffer.
     *
     * @return buffer input stream.
     */
    InputStream getInputStream();

    /**
     * Convert this buffer to file region.
     *
     * @return file region.
     */
    FileRegion toFileRegion();

    /**
     * Get the buffer size of this buffer.
     *
     * @return buffer size in bytes.
     */
    int getBufferSize();

    /**
     * Write data from a output stream.
     *
     * @param outputStream output stream.
     * @throws IOException io exception.
     */
    void write(OutputStream outputStream) throws IOException;

    /**
     * Set ref count, the number of consumer which handle this buffer.
     *
     * @param refCount ref count.
     */
    void setRefCount(int refCount);

    /**
     * Check if this buffer disposable.
     *
     * @return if disposable.
     */
    boolean isDisposable();

    /**
     * Release this buffer.
     */
    void release();

    interface BufferBuilder {

        /**
         * Get the OutputStream.
         *
         * @return output stream
         */
        OutputStream getOutputStream();

        /**
         * Set the position of the stream.
         *
         * @param position position
         */
        void positionStream(int position);

        /**
         * Get the buffer size.
         *
         * @return buffer size
         */
        int getBufferSize();

        /**
         * Get record count in the buffer.
         *
         * @return record count
         */
        long getRecordCount();

        /**
         * Increase the record count.
         */
        void increaseRecordCount();

        /**
         * Set memory track.
         */
        void enableMemoryTrack();

        /**
         * Build the buffer.
         *
         * @return buffer.
         */
        OutBuffer build();

        /**
         * Close this builder.
         */
        void close();

    }

}
