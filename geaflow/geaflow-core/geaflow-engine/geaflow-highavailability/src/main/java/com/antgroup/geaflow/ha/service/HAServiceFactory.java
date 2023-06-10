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

package com.antgroup.geaflow.ha.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAServiceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HAServiceFactory.class);
    private static IHAService haService;

    public static synchronized IHAService getService(Configuration configuration) {
        if (haService == null) {
            String serviceType = configuration.getString(ExecutionConfigKeys.HA_SERVICE_TYPE);
            if (StringUtils.isEmpty(serviceType)) {
                if (configuration.getBoolean(ExecutionConfigKeys.RUN_LOCAL_MODE)) {
                    serviceType = HAServiceType.memory.name();
                } else {
                    serviceType = HAServiceType.redis.name();
                }
            }
            haService = createHAService(serviceType);
            haService.open(configuration);
        }
        return haService;
    }

    public static IHAService getService() {
        if (haService == null) {
            throw new GeaflowRuntimeException("HAService not initialized");
        }
        return haService;
    }

    private static IHAService createHAService(String serviceType) {
        if (serviceType.equalsIgnoreCase(HAServiceType.redis.name())) {
            return new RedisHAService();
        } else {
            LOGGER.warn("unknown ha service type:{}, use default memoryHaService", serviceType);
            return new MemoryHAService();
        }
    }

}
