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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.Gauge;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricGroupRegistryTest {

    @Test
    public void test() {
        Configuration config = new Configuration();
        config.put(REPORTER_LIST, "mock");
        MetricGroupRegistry metricGroupRegistry = MetricGroupRegistry.getInstance(config);

        MetricGroup metricGroup = metricGroupRegistry.getMetricGroup();
        metricGroup.register("timestamp", new Gauge() {
            @Override
            public Object getValue() {
                return System.currentTimeMillis();
            }

            @Override
            public void setValue(Object value) {
            }
        });
        Assert.assertNotNull(metricGroup.gauge("timestamp"));

        MetricGroup namedMetricGroup = metricGroupRegistry.getMetricGroup("group");
        namedMetricGroup.register("timestamp", new Gauge() {
            @Override
            public Object getValue() {
                return System.currentTimeMillis();
            }

            @Override
            public void setValue(Object value) {
            }
        });
        Assert.assertNotNull(namedMetricGroup.gauge("group.timestamp"));
    }

}
