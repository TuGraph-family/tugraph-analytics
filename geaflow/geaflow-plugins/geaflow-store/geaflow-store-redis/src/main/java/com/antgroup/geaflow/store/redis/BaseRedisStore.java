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

package com.antgroup.geaflow.store.redis;

import com.antgroup.geaflow.store.AbstractBaseStore;
import com.antgroup.geaflow.store.IBaseStore;
import com.antgroup.geaflow.store.context.StoreContext;
import com.google.common.primitives.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

public abstract class BaseRedisStore extends AbstractBaseStore implements IBaseStore {

    protected static final Logger LOGGER = LoggerFactory.getLogger(KVRedisStore.class);
    protected static final char REDIS_NAMESPACE_SPLITTER = ':';
    protected transient JedisPool jedisPool;
    protected byte[] prefix;
    protected int retryTimes;
    protected int retryIntervalMs;

    public void init(StoreContext storeContext) {
        this.retryTimes = storeContext.getConfig().getInteger(RedisConfigKeys.REDIS_RETRY_TIMES);
        this.retryIntervalMs = storeContext.getConfig().getInteger(RedisConfigKeys.REDIS_RETRY_INTERVAL_MS);
        String host = storeContext.getConfig().getString(RedisConfigKeys.REDIS_HOST);
        int port = storeContext.getConfig().getInteger(RedisConfigKeys.REDIS_PORT);
        LOGGER.info("redis connect {}:{}", host, port);
        this.jedisPool = new JedisPool(host, port);
        String prefixStr = storeContext.getName() + REDIS_NAMESPACE_SPLITTER;
        this.prefix = prefixStr.getBytes();
    }

    protected byte[] getRedisKey(byte[] key) {
        return Bytes.concat(prefix, key);
    }

    @Override
    public void close() {
        this.jedisPool.close();
    }

    @Override
    public void drop() {

    }
}
