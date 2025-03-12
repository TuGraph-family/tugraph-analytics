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

package com.antgroup.geaflow.example.window;

import com.antgroup.geaflow.api.function.base.KeySelector;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.function.base.ReduceFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.RecoverableFileSource;
import com.antgroup.geaflow.example.util.EnvironmentUtil;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.callback.ICallbackFunction;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowCallBackPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WindowCallBackPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/count1";
    public static final String REF_FILE_PATH = "data/reference/count1";

    public static void main(String[] args) {
        Environment environment = EnvironmentUtil.loadEnvironment(args);
        submit(environment);
    }

    public static IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        TaskCallBack taskCallBack = pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource = pipelineTaskCxt.buildSource(
                    new RecoverableFileSource<String>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            return Collections.singletonList(fields[0]);
                        }) {}, SizeTumblingWindow.of(1000))
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .filter(e -> Integer.parseInt(e) > 100)
                    .map(e -> Tuple.of(e, 1))
                    .keyBy(new KeySelectorFunc())
                    .map(v -> String.format("(%s,%s)", ((Tuple) v).f0,
                        ((Tuple) v).f1))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
            }
        });
        taskCallBack.addCallBack(new ICallbackFunction() {
            @Override
            public void window(long windowId) {
                LOGGER.info("finish windowId:{}", windowId);
            }

            @Override
            public void terminal() {

            }
        });

        return pipeline.execute();
    }

    public static void validateResult(Comparator<String> comparator) throws IOException {
        ResultValidator.validateMapResult(REF_FILE_PATH, RESULT_FILE_PATH, comparator);
    }


    public static class MapFunc implements MapFunction<String, Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> map(String value) {
            LOGGER.info("MapFunc process value: {}", value);
            return Tuple.of(value, 1);
        }
    }

    public static class KeySelectorFunc implements KeySelector<Tuple<String, Integer>, Object> {

        @Override
        public Object getKey(Tuple<String, Integer> value) {
            return value.f0;
        }
    }

    public static class CountFunc implements ReduceFunction<Tuple<String, Integer>> {

        @Override
        public Tuple<String, Integer> reduce(Tuple<String, Integer> oldValue, Tuple<String, Integer> newValue) {
            return Tuple.of(oldValue.f0, oldValue.f1 + newValue.f1);
        }
    }
}
