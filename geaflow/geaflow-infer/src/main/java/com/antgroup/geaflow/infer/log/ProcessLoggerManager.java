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

import com.antgroup.geaflow.common.utils.ThreadUtil;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProcessLoggerManager implements AutoCloseable {

    private static final String PROCESS_LOG_PREFIX = "infer-process-log";

    private static final int PROCESS_THREAD_NUM = 2;

    private final Process process;

    private final ProcessOutputConsumer processOutputConsumer;

    private ProcessErrorOutputLogger errorOutputLogger;

    private final ExecutorService executor;

    public ProcessLoggerManager(Process process,
                                ProcessOutputConsumer processOutputConsumer) {
        this.process = process;
        this.processOutputConsumer = processOutputConsumer;
        this.executor = Executors.newFixedThreadPool(PROCESS_THREAD_NUM, ThreadUtil.namedThreadFactory(true, PROCESS_LOG_PREFIX));
    }

    public void startLogging() {
        this.executor.execute(new ProcessStdOutputLogger(process.getInputStream(),
            processOutputConsumer.getStdOutConsumer()));

        errorOutputLogger = new ProcessErrorOutputLogger(process.getErrorStream(),
            processOutputConsumer.getStdErrConsumer());
        this.executor.execute(errorOutputLogger);
    }

    public ProcessErrorOutputLogger getErrorOutputLogger() {
        return errorOutputLogger;
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
    }
}
