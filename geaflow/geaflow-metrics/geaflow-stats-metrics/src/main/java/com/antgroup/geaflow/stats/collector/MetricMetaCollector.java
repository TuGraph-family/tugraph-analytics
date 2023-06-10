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
import com.antgroup.geaflow.metrics.common.MetricMeta;
import com.antgroup.geaflow.stats.model.StatsMetricType;
import com.antgroup.geaflow.stats.sink.IStatsWriter;

public class MetricMetaCollector extends BaseStatsCollector {

    private static final String KEY_SPLIT = "_";

    private static final String META_KEY = "META";

    MetricMetaCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportMetricMeta(MetricMeta metricMeta) {
        addToWriterQueue(getMetricMetaKey(metricMeta.getMetricName()), metricMeta);
    }

    private String getMetricMetaKey(String metricName) {
        return jobName + StatsMetricType.Metrics.getValue() + META_KEY + KEY_SPLIT + metricName;
    }

}
