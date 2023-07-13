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

package com.antgroup.geaflow.cluster.failover;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverStrategyFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverStrategyFactory.class);

    public static IFailoverStrategy loadFailoverStrategy(EnvType envType, String foStrategyType) {
        ServiceLoader<IFailoverStrategy> contextLoader = ServiceLoader.load(IFailoverStrategy.class);
        Iterator<IFailoverStrategy> contextIterable = contextLoader.iterator();
        while (contextIterable.hasNext()) {
            IFailoverStrategy strategy = contextIterable.next();
            if (strategy.getEnv() == envType && strategy.getType().name().equalsIgnoreCase(foStrategyType)) {
                return strategy;
            }
        }
        LOGGER.error("NOT found IFoStrategy implementation with type:{}", foStrategyType);
        throw new GeaflowRuntimeException(
            RuntimeErrors.INST.spiNotFoundError(IFailoverStrategy.class.getSimpleName()));
    }
}
