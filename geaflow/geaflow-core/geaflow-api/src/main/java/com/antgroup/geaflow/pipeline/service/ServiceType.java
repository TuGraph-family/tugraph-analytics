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

package com.antgroup.geaflow.pipeline.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;

public enum ServiceType {

    /**
     * Rpc analytics service.
     */
    analytics_rpc,

    /**
     * Http analytics service.
     */
    analytics_http,

    /**
     * Storage service.
     */
    storage;

    public static ServiceType getEnum(String type) {
        for (ServiceType serviceType : values()) {
            if (serviceType.name().equalsIgnoreCase(type)) {
                return serviceType;
            }
        }
        return analytics_rpc;
    }

    public static ServiceType getEnum(Configuration config) {
        String type = config.getString(FrameworkConfigKeys.SERVICE_SERVER_TYPE);
        return getEnum(type);
    }

}
