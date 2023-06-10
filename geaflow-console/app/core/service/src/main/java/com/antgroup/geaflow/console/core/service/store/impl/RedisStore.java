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

package com.antgroup.geaflow.console.core.service.store.impl;

import com.antgroup.geaflow.console.core.model.job.config.HaMetaArgsClass;
import com.antgroup.geaflow.console.core.model.plugin.config.RedisPluginConfigClass;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.service.store.GeaflowHaMetaStore;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

@Slf4j
@Component
public class RedisStore implements GeaflowHaMetaStore {

    public static final char REDIS_NAMESPACE_SPLITTER = ':';

    private static final Integer MAX_SCAN_COUNT = Integer.MAX_VALUE;

    private static final Cache<String, Jedis> JEDIS_CACHE = CacheBuilder.newBuilder().initialCapacity(10)
        .maximumSize(100).expireAfterWrite(90, TimeUnit.SECONDS)
        .removalListener((RemovalListener<String, Jedis>) removalNotification -> {
            Jedis jedis = removalNotification.getValue();
            // async clean the resources
            CompletableFuture.runAsync(() -> {
                log.info("redis close {}:{}", jedis.getClient().getHost(), jedis.getClient().getPort());
                jedis.close();
            });
        }).build();

    @Override
    public void cleanHaMeta(GeaflowTask task) {
        RedisPluginConfigClass redisConfig = (RedisPluginConfigClass) new HaMetaArgsClass(
            task.getHaMetaPluginConfig()).getPlugin();

        String host = redisConfig.getHost();
        int port = redisConfig.getPort();
        String keyPattern = String.format("*%s*", task.getId());

        deleteByKeyPattern(host, port, keyPattern);
    }

    protected void deleteByKey(String host, int port, String key) {
        Jedis jedis = getJedis(host, port);
        jedis.del(key);
    }

    protected void deleteByKeyPattern(String host, int port, String keyPattern) {
        Jedis jedis = getJedis(host, port);
        Set<String> keySets = getScanKeySets(jedis, keyPattern, MAX_SCAN_COUNT);
        keySets.forEach(jedis::del);
    }

    protected Set<String> getScanKeySets(Jedis jedis, String keyPattern, Integer count) {
        Set<String> keySets = new HashSet<>();
        ScanParams scanParams = new ScanParams();
        scanParams.match(keyPattern);
        scanParams.count(count);
        // the start cursor
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanResult<String> scanResult;
        while (true) {
            scanResult = jedis.scan(cursor, scanParams);
            List<String> result = scanResult.getResult();
            if (CollectionUtils.isNotEmpty(result)) {
                keySets.addAll(result);
            }
            if ("0".equals(cursor)) {
                break;
            }
        }
        return keySets;
    }

    protected Jedis getJedis(String host, int port) {
        String cacheKey = host + REDIS_NAMESPACE_SPLITTER + port;
        Jedis jedis = JEDIS_CACHE.getIfPresent(cacheKey);
        if (jedis == null) {
            log.info("redis connect {}:{}", host, port);
            jedis = new Jedis(host, port);
            JEDIS_CACHE.put(cacheKey, jedis);
        }
        return jedis;
    }
}
