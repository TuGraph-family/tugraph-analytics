/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.operator.impl.window;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.window.incremental.IncrReduceOperator;
import com.antgroup.geaflow.state.StoreType;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncrReduceOperatorTest {

    private static final int BATCH_SIZE = 10;

    private IncrReduceOperator operator;
    private MyReduce reduce;
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
        this.reduce = new MyReduce();
        this.operator = new IncrReduceOperator(this.reduce, new KeySelectorFunc());
        this.operator.open(opContext);

        this.batchId2Value = new HashMap<>();
    }

    @Test
    public void testReduce() throws Exception {
        long batchId = 1;
        long value = 0;
        for (int i = 0; i < 1000; i++) {
            value += i;
            if ((i + 1) % BATCH_SIZE == 0) {
                this.batchId2Value.put(batchId++, value);
            }
        }

        batchId = 1;
        for (int i = 0; i < 1000; i++) {
            this.operator.processElement(i);
            if ((i + 1) % BATCH_SIZE == 0) {
                this.operator.finish();
                this.operator.checkpoint(batchId);
                batchId++;
                Assert.assertTrue(this.reduce.getValue() == this.batchId2Value.get(batchId - 1));
            }
        }
    }

    public static class KeySelectorFunc implements KeySelector<Integer, Integer> {

        @Override
        public Integer getKey(Integer value) {
            return 0;
        }
    }

    class MyReduce implements ReduceFunction<Integer> {

        private int value;

        public MyReduce() {
            this.value = 0;
        }

        @Override
        public Integer reduce(Integer oldValue, Integer newValue) {
            value += newValue;
            return oldValue + newValue;
        }

        public int getValue() {
            return value;
        }
    }
}
