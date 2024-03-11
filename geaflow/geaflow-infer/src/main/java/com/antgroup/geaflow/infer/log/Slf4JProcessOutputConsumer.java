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
package com.antgroup.geaflow.infer.log;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4JProcessOutputConsumer implements ProcessOutputConsumer {

    private final Logger logger;

    public Slf4JProcessOutputConsumer(String loggerName) {
        logger = LoggerFactory.getLogger(loggerName);
    }

    @Override
    public Consumer<String> getStdOutConsumer() {
        return logger::info;
    }

    @Override
    public Consumer<String> getStdErrConsumer() {
        return logger::error;
    }
}
