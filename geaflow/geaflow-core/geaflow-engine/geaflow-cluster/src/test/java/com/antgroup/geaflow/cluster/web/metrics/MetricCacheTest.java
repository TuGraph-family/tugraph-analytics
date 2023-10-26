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

package com.antgroup.geaflow.cluster.web.metrics;

import com.antgroup.geaflow.common.metric.CycleMetrics;
import com.antgroup.geaflow.common.metric.PipelineMetrics;
import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.antgroup.geaflow.stats.model.MetricCache;
import org.junit.Test;
import org.testng.Assert;

public class MetricCacheTest {

    @Test
    public void test() {
        PipelineMetrics pipelineMetrics = new PipelineMetrics("1");
        MetricCache metricCache = new MetricCache();
        metricCache.addPipelineMetrics(pipelineMetrics);
        CycleMetrics cycleMetrics = new CycleMetrics("1", "1", "");
        metricCache.addCycleMetrics(cycleMetrics);
        byte[] bytes = SerializerFactory.getKryoSerializer().serialize(metricCache);
        MetricCache newCache = (MetricCache) SerializerFactory.getKryoSerializer().deserialize(bytes);
        Assert.assertNotNull(newCache);
        Assert.assertEquals(newCache.getPipelineMetricCaches().size(), 1);
    }

}
