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

package com.antgroup.geaflow.metrics.reporter;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.metrics.common.MetricConfig;
import com.antgroup.geaflow.metrics.common.MetricMeta;
import com.antgroup.geaflow.metrics.common.MetricType;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricMetaClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricMetaClient.class);
    private static MetricMetaClient reporterClient;

    private int failNum = 0;
    private final int maxRetries;
    private final String jobName;
    private final List<MetricMeta> metricList = new ArrayList<>();
    private final BlockingQueue<MetricMeta> metricMetaQueue = new LinkedBlockingQueue<>();
    private final MetricConfig metricConfig;
    private final ScheduledExecutorService scheduledService;

    private MetricMetaClient(Configuration config) {
        this.jobName = config.getString(ExecutionConfigKeys.JOB_APP_NAME);

        this.metricConfig = new MetricConfig(config);
        this.maxRetries = metricConfig.getReportMaxRetries();
        this.scheduledService = new ScheduledThreadPoolExecutor(1,
            ThreadUtil.namedThreadFactory(true, "async-metric-meta"));
        scheduledService
            .scheduleAtFixedRate(new RegisterTask(), metricConfig.getRandomDelaySec(),
                metricConfig.getRandomPeriodSec(), TimeUnit.SECONDS);
    }

    public static synchronized MetricMetaClient getInstance(Configuration config) {
        if (reporterClient == null) {
            reporterClient = new MetricMetaClient(config);
        }
        return reporterClient;
    }

    public void registerMetricMeta(String metricName, MetricType metricType, String queries) {
        MetricMeta metricMeta = new MetricMeta();
        metricMeta.setJobName(jobName);
        metricMeta.setMetricName(metricName);
        metricMeta.setQueries(queries);
        metricMeta.setMetricType(metricType.name());
        metricMetaQueue.add(metricMeta);
    }

    public void close() {
        if (scheduledService != null) {
            scheduledService.shutdown();
        }
    }

    private class RegisterTask implements Runnable {

        @Override
        public void run() {
            try {
                if (metricMetaQueue.size() > 0 || metricList.size() > 0) {
                    if (metricList.size() == 0) {
                        metricMetaQueue.drainTo(metricList);
                    }
                    SleepUtils.sleepSecond(metricConfig.getRandomPeriodSec());
                    for (MetricMeta metricMeta : metricList) {
                        StatsCollectorFactory.getInstance().getMetricMetaCollector().reportMetricMeta(metricMeta);
                        LOGGER.info("register {} with query: {}", metricMeta.getMetricName(),
                            metricMeta.getQueries());
                    }
                    metricList.clear();
                    failNum = 0;
                }
            } catch (RuntimeException ex) {
                failNum++;
                if (failNum < maxRetries) {
                    LOGGER.warn("register fail #{}, and retry in next round", failNum, ex);
                } else {
                    LOGGER.warn("#{} retry exceeds {} times, discard {} metrics meta", failNum,
                        maxRetries, metricList.size());
                    metricList.clear();
                    failNum = 0;
                }
                throw ex;
            }
        }
    }

}
