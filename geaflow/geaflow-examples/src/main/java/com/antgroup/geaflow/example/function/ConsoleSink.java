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

package com.antgroup.geaflow.example.function;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.io.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleSink<OUT> extends RichFunction implements SinkFunction<OUT>  {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleSink.class);

    private int taskIndex;

    @Override
    public void open(RuntimeContext runtimeContext) {
        this.taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
    }

    @Override
    public void write(OUT out) throws Exception {
        LOGGER.info("sink {} got result {}", this.taskIndex, out);
    }

    @Override
    public void close() {
    }

}
