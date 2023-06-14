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

package com.antgroup.geaflow.cluster.exception;

import com.antgroup.geaflow.common.thread.Executors;
import com.antgroup.geaflow.common.utils.ExecutorUtil;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionCollectService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionCollectService.class);

    private static final String EXCEPTION_COLLECT_FORMAT = "geaflow-exception-collect-%d";
    private static final int EXCEPTION_COLLECTOR_THREAD_NUM = 1;

    private ExecutorService exceptionCollectService;
    private ComponentExceptionSupervisor supervisor;

    public ExceptionCollectService() {
        this.exceptionCollectService =
            Executors.getExecutorService(EXCEPTION_COLLECTOR_THREAD_NUM, EXCEPTION_COLLECT_FORMAT);
        supervisor = ComponentExceptionSupervisor.getInstance();
        this.exceptionCollectService.execute(supervisor);
    }

    public void shutdown() {

        LOGGER.info("shutdown exception collect service");
        if (supervisor != null) {
            supervisor.shutdown();
        }
        if (exceptionCollectService != null) {
            ExecutorUtil.shutdown(exceptionCollectService);
        }
    }
}
