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

package com.antgroup.geaflow.shuffle.network;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import com.antgroup.geaflow.shuffle.config.ShuffleConfig;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public interface IConnectionManager {

    /**
     * Get the shuffle config.
     *
     * @return shuffle config.
     */
    ShuffleConfig getShuffleConfig();

    /**
     * Client buffer.
     *
     * @return buffer.
     */
    PooledByteBufAllocator getClientBufAllocator();

    /**
     * Server buffer.
     *
     * @return buffer.
     */
    PooledByteBufAllocator getServerBufAllocator();

    /**
     * Get the shuffle address.
     *
     * @return shuffle address.
     */
    ShuffleAddress getShuffleAddress();

    /**
     * Close connection manager.
     *
     * @throws IOException io exception.
     */
    void close() throws IOException;

    /**
     * Get the thread pool to async callback action.
     *
     * @return thread pool.
     */
    ExecutorService getExecutor();

}
