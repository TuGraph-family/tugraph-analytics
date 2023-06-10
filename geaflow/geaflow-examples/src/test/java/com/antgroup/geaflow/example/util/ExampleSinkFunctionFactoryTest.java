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

package com.antgroup.geaflow.example.util;

import com.antgroup.geaflow.api.function.io.SinkFunction;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.example.function.ConsoleSink;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.util.ExampleSinkFunctionFactory.SinkType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExampleSinkFunctionFactoryTest {

    @Test
    public void test() {
        Configuration configuration = new Configuration();
        configuration.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.CONSOLE_SINK.name());

        SinkFunction<Object> sinkFunction = ExampleSinkFunctionFactory.getSinkFunction(configuration);
        Assert.assertTrue(sinkFunction instanceof ConsoleSink);
    }

    @Test
    public void test1() {
        Configuration configuration = new Configuration();
        configuration.put(ExampleConfigKeys.GEAFLOW_SINK_TYPE.getKey(), SinkType.FILE_SINK.name());

        SinkFunction<Object> sinkFunction = ExampleSinkFunctionFactory.getSinkFunction(configuration);
        Assert.assertTrue(sinkFunction instanceof FileSink);
    }
}
