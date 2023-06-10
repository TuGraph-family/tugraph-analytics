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
import com.antgroup.geaflow.common.metric.CycleMetrics;
import com.antgroup.geaflow.common.metric.PipelineMetrics;
import com.antgroup.geaflow.stats.model.StatsMetricType;
import com.antgroup.geaflow.stats.sink.IStatsWriter;

public class PipelineStatsCollector extends BaseStatsCollector {

    private static final String KEY_SPLIT = "_";

    PipelineStatsCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportPipelineMetrics(PipelineMetrics pipelineMetric) {
        addToWriterQueue(genMetricKey(PipelineMetricsType.PIPELINE, pipelineMetric.getName()), pipelineMetric);
    }

    public void reportCycleMetrics(CycleMetrics cycleMetrics) {
        String name = cycleMetrics.getPipelineName() + KEY_SPLIT + cycleMetrics.getName();
        addToWriterQueue(genMetricKey(PipelineMetricsType.CYCLE, name), cycleMetrics);
    }

    private String genMetricKey(PipelineMetricsType type, String name) {
        return jobName + StatsMetricType.Metrics.getValue() + type + KEY_SPLIT + name;
    }

    private enum PipelineMetricsType {
        PIPELINE,
        CYCLE
    }
}
