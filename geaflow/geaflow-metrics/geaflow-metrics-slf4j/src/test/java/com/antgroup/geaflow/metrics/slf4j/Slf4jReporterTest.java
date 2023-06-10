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

package com.antgroup.geaflow.metrics.slf4j;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.Gauge;
import com.antgroup.geaflow.metrics.common.api.Histogram;
import com.antgroup.geaflow.metrics.common.api.Meter;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.metrics.common.reporter.MetricReporter;
import com.antgroup.geaflow.metrics.common.reporter.ReporterRegistry;
import com.antgroup.geaflow.metrics.common.reporter.ScheduledReporter;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Slf4jReporterTest {

    @Test
    public void test() {
        Configuration config = new Configuration();
        config.put(REPORTER_LIST, ReporterRegistry.SLF4J_REPORTER);
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

        Histogram histogram = metricGroup.histogram("histTest");
        histogram.update(1);
        Meter meter = metricGroup.meter("meterTest");
        meter.mark();

        List<MetricReporter> reporterList = metricGroupRegistry.getReporterList();
        Assert.assertNotNull(reporterList);
        for (MetricReporter reporter : reporterList) {
            ((ScheduledReporter) reporter).report();
            reporter.close();
        }
    }

}
