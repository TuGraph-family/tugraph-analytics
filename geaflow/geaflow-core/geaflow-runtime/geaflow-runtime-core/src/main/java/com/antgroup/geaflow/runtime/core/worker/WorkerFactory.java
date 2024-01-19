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

package com.antgroup.geaflow.runtime.core.worker;

import com.antgroup.geaflow.cluster.worker.IWorker;
import com.antgroup.geaflow.cluster.worker.WorkerType;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerFactory.class);

    public static IWorker createWorker(Configuration configuration) {
        WorkerType workerType = configuration.getBoolean(FrameworkConfigKeys.ASP_ENABLE)
            ? WorkerType.unaligned_compute : WorkerType.aligned_compute;

        ServiceLoader<IWorker> executorLoader = ServiceLoader.load(IWorker.class);
        Iterator<IWorker> executorIterable = executorLoader.iterator();
        while (executorIterable.hasNext()) {
            IWorker worker = executorIterable.next();
            if (worker.getWorkerType() == workerType) {
                return worker;
            }
        }
        LOGGER.error("NOT found IWorker implementation");
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(IWorker.class.getSimpleName()));
    }
}
