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

package com.antgroup.geaflow.console.core.service.statement;

import com.antgroup.geaflow.console.common.service.integration.engine.analytics.AnalyticsClient;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AnalyticsClientPool {

    @Autowired
    private AnalyticsClientFactory analyticsClientFactory;

    private static final LoadingCache<String, LinkedBlockingQueue<AnalyticsClient>> clientPool = CacheBuilder.newBuilder().maximumSize(100)
        .expireAfterWrite(180, TimeUnit.SECONDS).build(new CacheLoader<String, LinkedBlockingQueue<AnalyticsClient>>() {
            @Override
            public LinkedBlockingQueue<AnalyticsClient> load(String jobId) {
                return new LinkedBlockingQueue<>(10);
            }
        });

    public AnalyticsClient getClient(GeaflowTask task) {
        try {
            LinkedBlockingQueue<AnalyticsClient> analyticsClients = clientPool.get(task.getId());
            AnalyticsClient client = analyticsClients.poll();
            if (client == null) {
                client = analyticsClientFactory.buildClient(task);
            }
            return client;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    public void addClient(GeaflowTask task, AnalyticsClient client) {
        try {
            LinkedBlockingQueue<AnalyticsClient> analyticsClients = clientPool.get(task.getId());
            analyticsClients.offer(client);
        } catch (Exception e) {
            log.info("add client fail", e);
        }
    }


}
