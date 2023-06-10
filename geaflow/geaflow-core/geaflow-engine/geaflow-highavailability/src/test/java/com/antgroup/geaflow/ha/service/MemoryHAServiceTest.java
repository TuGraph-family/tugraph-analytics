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
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryHAServiceTest {

    @Test
    public void test() {
        MemoryHAService memoryHAService = new MemoryHAService();
        memoryHAService.open(new Configuration());

        ResourceData resourceData = new ResourceData();
        memoryHAService.register("1", resourceData);
        Assert.assertEquals(resourceData, memoryHAService.resolveResource("1"));
        Assert.assertNull(memoryHAService.resolveResource("2"));
        memoryHAService.invalidateResource("1");
        Assert.assertNull(memoryHAService.resolveResource("1"));
    }

}
