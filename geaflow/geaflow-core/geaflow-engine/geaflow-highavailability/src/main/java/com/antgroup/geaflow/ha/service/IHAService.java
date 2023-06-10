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
import java.io.Serializable;

public interface IHAService extends Serializable {

    /**
     * HA service init.
     */
    void open(Configuration configuration);

    /**
     * Register resource data.
     */
    void register(String resourceId, ResourceData resourceData);

    /**
     * Returns resource data for corresponding resource id.
     */
    ResourceData resolveResource(String resourceId);

    /**
     * Invalidate resource data for corresponding resource id.
     */
    void invalidateResource(String resourceId);

    /**
     * HA service close.
     */
    void close();

}
