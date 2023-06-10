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

package com.antgroup.geaflow.ha.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.store.api.key.IKVStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHAService implements IHAService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHAService.class);
    private static final int DEFAULT_TIMEOUT = 3000;
    protected static final String TABLE_PREFIX = "WORKERS_";

    protected int recoverTimeout;
    protected Map<String, ResourceData> resourceDataCache;
    protected IKVStore<String, ResourceData> kvStore;

    public AbstractHAService() {
        this.resourceDataCache = new ConcurrentHashMap<>();
    }

    @Override
    public void open(Configuration configuration) {
        this.recoverTimeout = configuration.getInteger(ExecutionConfigKeys.FO_TIMEOUT_MS);
    }

    @Override
    public void register(String resourceId, ResourceData resourceData) {
        kvStore.put(resourceId, resourceData);
    }

    @Override
    public ResourceData resolveResource(String resourceId) {
        return resourceDataCache.computeIfAbsent(resourceId, this::loadDataFromStore);
    }

    @Override
    public void invalidateResource(String resourceId) {
        resourceDataCache.remove(resourceId);
    }

    @Override
    public void close() {
        if (kvStore != null) {
            kvStore.close();
        }
    }

    protected ResourceData getResourceData(String resourceId) {
        return kvStore.get(resourceId);
    }

    private ResourceData loadDataFromStore(String resourceId) {
        long currentTime = System.currentTimeMillis();
        long startTime = currentTime;
        long checkTime = currentTime;
        Throwable throwable = null;
        ResourceData resourceData = null;
        do {
            currentTime = System.currentTimeMillis();
            if (currentTime - checkTime > 2000) {
                long elapsedTime = currentTime - startTime;
                LOGGER.warn("failed to resolve resource:{} resourceData:{}", resourceId,
                    resourceData, throwable);
                checkTime = currentTime;
                if (elapsedTime > recoverTimeout) {
                    String msg = String.format("load resource %s timeout after %sms", resourceId,
                        elapsedTime);
                    LOGGER.error(msg);
                    throw new GeaflowRuntimeException(msg);
                }
                SleepUtils.sleepMilliSecond(200);
            }
            resourceData = getResourceData(resourceId);
            if (resourceData != null) {
                try {
                    checkServiceAvailable(resourceData.getHost(), resourceData.getRpcPort());
                    break;
                } catch (IOException ex) {
                    throwable = ex;
                }
            }
        } while (true);
        return resourceData;
    }

    private void checkServiceAvailable(String hostName, int port) throws IOException {
        try (Socket socket = new Socket()) {
            InetSocketAddress socketAddress = new InetSocketAddress(hostName, port);
            socket.connect(socketAddress, DEFAULT_TIMEOUT);
        } catch (IOException ex) {
            throw ex;
        }
    }

}
