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

package com.antgroup.geaflow.operator.impl.window;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.base.AggregateFunction;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.window.incremental.IncrAggregateOperator;
import com.antgroup.geaflow.state.StoreType;
import java.util.HashMap;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class IncrAggregateOperatorTest {

    private static int batchSize = 10;

    private IncrAggregateOperator operator;
    private MyAgg agg;
    private Operator.OpContext opContext;
    private Map<Long, Long> batchId2Value;

    @BeforeMethod
    public void setup() {
        ICollector collector = mock(ICollector.class);
        Configuration configuration = new Configuration();
        configuration.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        when(runtimeContext.getConfiguration()).thenReturn(configuration);
        when(runtimeContext.getTaskArgs()).thenReturn(new TaskArgs(1, 0, "agg", 1, 1024, 0));
        when(runtimeContext.clone(any(Map.class))).thenReturn(runtimeContext);
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
        MetricGroup metricGroup = MetricGroupRegistry.getInstance(config).getMetricGroup();
        Mockito.doReturn(metricGroup).when(runtimeContext).getMetric();
        Mockito.doReturn(runtimeContext).when(runtimeContext).clone(any());
        this.opContext = new AbstractOperator.DefaultOpContext(
            Lists.newArrayList(collector), runtimeContext);
        this.agg = new MyAgg();
        this.operator = new IncrAggregateOperator(this.agg, new KeySelectorFunc());
        this.operator.open(opContext);

        this.batchId2Value = new HashMap<>();
    }

    @Test
    public void testAgg() throws Exception {
        long batchId = 1;
        long value = 0;
        for (int i = 0; i < 1000; i++) {
            value += i;
            if ((i + 1) % batchSize == 0) {
                this.batchId2Value.put(batchId++, value);
            }
        }

        batchId = 1;
        for (int i = 0; i < 1000; i++) {
            this.operator.processElement((long) i);
            if ((i + 1) % batchSize == 0) {
                this.operator.finish(batchId);
                this.operator.checkpoint(batchId);
                batchId++;
                Assert.assertTrue(this.agg.getValue() == this.batchId2Value.get(batchId - 1));
            }
        }
    }

    public static class KeySelectorFunc implements KeySelector<Long, Long> {

        @Override
        public Long getKey(Long value) {
            return value;
        }
    }

    class MutableLong {

        long value;
    }

    class MyAgg implements AggregateFunction<Long, MutableLong, Long> {

        private long value;

        public MyAgg() {
            this.value = 0;
        }

        @Override
        public MutableLong createAccumulator() {
            return new MutableLong();
        }

        @Override
        public void add(Long value, MutableLong accumulator) {
            accumulator.value += value;
            this.value += value;
        }

        @Override
        public Long getResult(MutableLong accumulator) {
            return accumulator.value;
        }

        @Override
        public MutableLong merge(MutableLong a, MutableLong b) {
            return null;
        }

        public long getValue() {
            return this.value;
        }
    }

}
