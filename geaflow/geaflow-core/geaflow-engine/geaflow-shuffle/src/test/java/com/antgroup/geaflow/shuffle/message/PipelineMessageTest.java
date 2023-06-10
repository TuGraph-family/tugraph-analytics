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

package com.antgroup.geaflow.shuffle.message;

import com.antgroup.geaflow.common.serialize.SerializerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PipelineMessageTest {

    @Test
    public void test() {
        PipelineMessage message = new PipelineMessage(3, "stream", null);
        byte[] bytes = SerializerFactory.getKryoSerializer().serialize(message);
        PipelineMessage result = (PipelineMessage) SerializerFactory.getKryoSerializer().deserialize(bytes);
        Assert.assertEquals(3, result.getWindowId(), "windowId should be ignored");
    }

}
