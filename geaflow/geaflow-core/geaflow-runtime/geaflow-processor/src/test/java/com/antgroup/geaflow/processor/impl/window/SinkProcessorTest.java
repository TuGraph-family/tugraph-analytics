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

package com.antgroup.geaflow.processor.impl.window;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.trait.TransactionTrait;
import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.task.TaskArgs;
import com.antgroup.geaflow.metrics.common.MetricGroupRegistry;
import com.antgroup.geaflow.metrics.common.api.MetricGroup;
import com.antgroup.geaflow.model.record.BatchRecord;
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.window.SinkOperator;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

public class SinkProcessorTest {

    private OneInputProcessor oneInputProcessor;
    private SinkOperator operator;
    private TransactionSinkFunction sinkFunction;
    private CommonSinkFunction commonSinkFunction;
    private Operator.OpContext opContext;

    @BeforeClass
    public void setup() {
        ICollector collector = mock(ICollector.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        when(runtimeContext.getConfiguration()).thenReturn(new Configuration());
        when(runtimeContext.getTaskArgs()).thenReturn(new TaskArgs(1, 0, "sink", 1, 1024, 0));
        when(runtimeContext.clone(any(Map.class))).thenReturn(runtimeContext);
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.REPORTER_LIST.getKey(), "");
        MetricGroup metricGroup = MetricGroupRegistry.getInstance(config).getMetricGroup();
        Mockito.doReturn(metricGroup).when(runtimeContext).getMetric();
        Mockito.doReturn(runtimeContext).when(runtimeContext).clone(any());
        this.opContext = new AbstractOperator.DefaultOpContext(
            Lists.newArrayList(collector), runtimeContext);
    }

    @Test
    public void testWriteAndFinishWithTransactionSink() throws Exception {
        this.sinkFunction = new TransactionSinkFunction();
        this.operator = new SinkOperator(this.sinkFunction);
        this.oneInputProcessor = new OneInputProcessor(this.operator);
        this.oneInputProcessor.open(this.opContext.getCollectors(), this.opContext.getRuntimeContext());

        for (int i = 0; i < 103; i++) {
            RecordArgs recordArgs = new RecordArgs(1);
            this.oneInputProcessor.process(new BatchRecord<>(recordArgs, Arrays.asList(i).iterator()));
        }
        Assert.assertEquals(this.sinkFunction.getList().size(), 3);
        this.oneInputProcessor.finish(1L);
        Assert.assertEquals(this.sinkFunction.getList().size(), 0);
    }

    @Test
    public void testWriteAndFinishWithCommonSink() throws Exception {
        this.commonSinkFunction = new CommonSinkFunction();
        this.operator = new SinkOperator(this.commonSinkFunction);
        this.oneInputProcessor = new OneInputProcessor(this.operator);
        this.oneInputProcessor.open(this.opContext.getCollectors(), this.opContext.getRuntimeContext());

        RecordArgs recordArgs = new RecordArgs(1);
        for (int i = 0; i < 103; i++) {
            this.oneInputProcessor.process(new BatchRecord<>(recordArgs, Arrays.asList(i).iterator()));
        }
        Assert.assertEquals(this.commonSinkFunction.getList().size(), 3);
        this.oneInputProcessor.finish(1L);
        Assert.assertEquals(this.commonSinkFunction.getList().size(), 3);
    }

    static class TransactionSinkFunction extends RichFunction implements SinkFunction<Integer>, Closeable, TransactionTrait {

        private List<Integer> list;
        private int num;

        @Override
        public void open(RuntimeContext runtimeContext) {
            list = new ArrayList<>();
            num = 1;
        }

        @Override
        public void close() {

        }

        @Override
        public void write(Integer value) throws Exception {
            list.add(value);
            if (num++ % 10 == 0) {
                list.clear();
                num = 1;
            }
        }

        @Override
        public void finish(long windowId) {
            list.clear();
        }

        @Override
        public void rollback(long windowId) {

        }

        public List<Integer> getList() {
            return list;
        }
    }

    static class CommonSinkFunction extends RichFunction implements SinkFunction<Integer>, Closeable {

        private List<Integer> list;
        private int num;

        @Override
        public void open(RuntimeContext runtimeContext) {
            list = new ArrayList<>();
            num = 1;
        }

        @Override
        public void close() {

        }

        @Override
        public void write(Integer value) throws Exception {
            list.add(value);
            if (num++ % 10 == 0) {
                list.clear();
                num = 1;
            }
        }

        public List<Integer> getList() {
            return list;
        }
    }
}
