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

package com.antgroup.geaflow.example.stream;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamWordCountCallBackPipeline implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamWordCountCallBackPipeline.class);

    public static final String RESULT_FILE_PATH = "./target/tmp/data/result/wordcount";
    public static final String REF_FILE_PATH = "data/reference/wordcount";
    private static final List<Long> taskCallBackList = new ArrayList<>();

    public IPipelineResult submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = ((EnvironmentContext) environment.getEnvironmentContext()).getConfig();
        envConfig.getConfigMap().put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);
        ResultValidator.cleanResult(RESULT_FILE_PATH);
        TaskCallBack taskCallBack = pipeline.submit(new PipelineTask() {
            @Override
            public void execute(IPipelineTaskContext pipelineTaskCxt) {
                Configuration conf = pipelineTaskCxt.getConfig();
                PWindowSource<String> streamSource = pipelineTaskCxt.buildSource(
                    new FileSource<String>("data/input/email_edge",
                        line -> {
                            String[] fields = line.split(",");
                            return Collections.singletonList(fields[0]);
                        }) {}, SizeTumblingWindow.of(5000))
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

                SinkFunction<String> sink = ExampleSinkFunctionFactory.getSinkFunction(conf);
                streamSource
                    .map(e -> Tuple.of(e, 1))
                    .keyBy(e -> e.f0)
                    .reduce((x, y) -> Tuple.of(x.f0, x.f1 + y.f1))
                    .withParallelism(conf.getInteger(ExampleConfigKeys.REDUCE_PARALLELISM))
                    .map(v -> String.format("(%s,%s)", ((Tuple) v).f0, ((Tuple) v).f1))
                    .sink(sink)
                    .withParallelism(conf.getInteger(ExampleConfigKeys.SINK_PARALLELISM));
            }
        });

        taskCallBackList.clear();
        taskCallBack.addCallBack(new ICallbackFunction() {
            @Override
            public void window(long windowId) {
                LOGGER.info("finish windowId:{}", windowId);
                taskCallBackList.add(windowId);
            }

            @Override
            public void terminal() {

            }
        });

        return pipeline.execute();
    }

    public static void validateResult() throws IOException {
        ResultValidator.validateResult(REF_FILE_PATH, RESULT_FILE_PATH);
        Assert.assertTrue("task call back should handle window", !taskCallBackList.isEmpty());
    }
}
