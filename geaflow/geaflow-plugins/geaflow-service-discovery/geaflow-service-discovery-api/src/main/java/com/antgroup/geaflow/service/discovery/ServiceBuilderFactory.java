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

package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceBuilderFactory {

    private static final Map<String, ServiceBuilder> CONCURRENT_TYPE_MAP = new ConcurrentHashMap<>();

    public static synchronized ServiceBuilder build(String serviceType) {
        if (CONCURRENT_TYPE_MAP.containsKey(serviceType)) {
            return CONCURRENT_TYPE_MAP.get(serviceType);
        }

        ServiceLoader<ServiceBuilder> serviceLoader = ServiceLoader.load(ServiceBuilder.class);
        for (ServiceBuilder storeBuilder: serviceLoader) {
            if (storeBuilder.serviceType().equalsIgnoreCase(serviceType)) {
                CONCURRENT_TYPE_MAP.put(serviceType, storeBuilder);
                return storeBuilder;
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(serviceType));
    }

    public static synchronized void clear() {
        CONCURRENT_TYPE_MAP.clear();
    }
}
