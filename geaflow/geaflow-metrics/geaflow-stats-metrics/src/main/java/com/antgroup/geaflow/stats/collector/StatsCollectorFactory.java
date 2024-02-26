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

package com.antgroup.geaflow.stats.collector;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.stats.model.MetricCache;
import com.antgroup.geaflow.stats.sink.IStatsWriter;
import com.antgroup.geaflow.stats.sink.StatsWriterFactory;

public class StatsCollectorFactory {

    private final ExceptionCollector exceptionCollector;
    private final EventCollector eventCollector;
    private final PipelineStatsCollector pipelineStatsCollector;
    private final ProcessStatsCollector processStatsCollector;
    private final MetricMetaCollector metricMetaCollector;
    private final HeartbeatCollector heartbeatCollector;
    private final MetricCache metricCache;
    private final IStatsWriter syncWriter;
    private static StatsCollectorFactory INSTANCE;

    private StatsCollectorFactory(Configuration configuration) {
        this.syncWriter = StatsWriterFactory.getStatsWriter(configuration, true);
        this.exceptionCollector = new ExceptionCollector(syncWriter, configuration);
        this.eventCollector = new EventCollector(syncWriter, configuration);
        this.metricCache = new MetricCache(configuration);
        IStatsWriter statsWriter = StatsWriterFactory.getStatsWriter(configuration);
        this.pipelineStatsCollector = new PipelineStatsCollector(statsWriter, configuration, metricCache);
        this.metricMetaCollector = new MetricMetaCollector(statsWriter, configuration);
        this.processStatsCollector = new ProcessStatsCollector(configuration);
        this.heartbeatCollector = new HeartbeatCollector(statsWriter, configuration);
    }

    public static synchronized StatsCollectorFactory init(Configuration configuration) {
        if (INSTANCE == null) {
            INSTANCE = new StatsCollectorFactory(configuration);
        }
        return INSTANCE;
    }

    public static StatsCollectorFactory getInstance() {
        return INSTANCE;
    }

    public ExceptionCollector getExceptionCollector() {
        return exceptionCollector;
    }

    public EventCollector getEventCollector() {
        return eventCollector;
    }

    public PipelineStatsCollector getPipelineStatsCollector() {
        return pipelineStatsCollector;
    }

    public HeartbeatCollector getHeartbeatCollector() {
        return heartbeatCollector;
    }

    public MetricMetaCollector getMetricMetaCollector() {
        return metricMetaCollector;
    }

    public ProcessStatsCollector getProcessStatsCollector() {
        return processStatsCollector;
    }

    public MetricCache getMetricCache() {
        return metricCache;
    }

    public IStatsWriter getStatsWriter() {
        return syncWriter;
    }

}
