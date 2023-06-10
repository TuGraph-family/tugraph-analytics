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

package com.antgroup.geaflow.cluster.executor;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineExecutorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineExecutorFactory.class);

    public static IPipelineExecutor createPipelineExecutor() {
        ServiceLoader<IPipelineExecutor> executorLoader = ServiceLoader.load(IPipelineExecutor.class);
        Iterator<IPipelineExecutor> executorIterable = executorLoader.iterator();
        while (executorIterable.hasNext()) {
            return executorIterable.next();
        }
        LOGGER.error("NOT found IPipelineExecutor implementation");
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IPipelineExecutor.class.getSimpleName()));
    }
}
