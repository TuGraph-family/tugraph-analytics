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

package com.antgroup.geaflow.view.stream;

import com.antgroup.geaflow.api.partition.kv.KeyByPartition;
import com.antgroup.geaflow.view.IViewDesc;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StreamViewDescTest {

    @Test
    public void testStreamViewDesc() {
        StreamViewDesc desc = new StreamViewDesc("view", 1, IViewDesc.BackendType.Memory);
        Assert.assertTrue(desc.getBackend().equals(IViewDesc.BackendType.Memory));
        Assert.assertTrue(desc.getName().equals("view"));
        Assert.assertTrue(desc.getShardNum() == 1);
        Assert.assertNull(desc.getViewProps());
    }

    @Test
    public void testStreamViewDescWithPartition() {
        StreamViewDesc desc = new StreamViewDesc("view", 1, IViewDesc.BackendType.Memory, new KeyByPartition(2), new HashMap());
        Assert.assertTrue(desc.getBackend().equals(IViewDesc.BackendType.Memory));
        Assert.assertTrue(desc.getName().equals("view"));
        Assert.assertTrue(desc.getShardNum() == 1);
        Assert.assertTrue(desc.getViewProps().size() == 0);
    }
}
