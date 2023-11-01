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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ServiceTypeTest {

    @Test
    public void testServiceType() {
        String serviceTypeName = ServiceType.analytics_rpc.name();
        Assert.assertTrue(ServiceType.getEnum(serviceTypeName) == ServiceType.analytics_rpc);
        serviceTypeName = ServiceType.storage.name();
        Assert.assertTrue(ServiceType.getEnum(serviceTypeName) == ServiceType.storage);
        serviceTypeName = null;
        Assert.assertTrue(ServiceType.getEnum(serviceTypeName) == ServiceType.analytics_rpc);

        serviceTypeName = ServiceType.analytics_http.name();
        Assert.assertTrue(ServiceType.getEnum(serviceTypeName) == ServiceType.analytics_http);
    }
}
