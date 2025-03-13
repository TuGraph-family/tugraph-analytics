/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.shuffle.pipeline.channel;

import com.antgroup.geaflow.shuffle.pipeline.buffer.PipeChannelBuffer;
import java.io.IOException;
import java.util.Optional;

public interface InputChannel {

    /**
     * request batches from upstream slices.
     * @param batchId the maximum batchId to fetch.
     * @throws IOException IO exception
     * @throws InterruptedException Interrupt exception
     */
    void requestSlice(long batchId) throws IOException, InterruptedException;

    /**
     * Returns the next buffer from the consumed slice or {@code Optional.empty()} if there
     * is no data to return.
     */
    Optional<PipeChannelBuffer> getNext() throws IOException, InterruptedException;

    /**
     * check if channel is released.
     * @return true if released.
     */
    boolean isReleased();

    /**
     * Releases all resources of the channel.
     */
    void release() throws IOException;

}
