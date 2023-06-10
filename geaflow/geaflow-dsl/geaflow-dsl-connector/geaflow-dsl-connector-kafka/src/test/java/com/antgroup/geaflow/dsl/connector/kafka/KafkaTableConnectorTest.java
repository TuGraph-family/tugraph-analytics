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

package com.antgroup.geaflow.dsl.connector.kafka;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.dsl.connector.api.function.OffsetStore.ConsoleOffset;
import com.antgroup.geaflow.dsl.connector.kafka.KafkaTableSource.KafkaOffset;
import com.antgroup.geaflow.dsl.connector.kafka.KafkaTableSource.KafkaPartition;
import java.io.IOException;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaTableConnectorTest {

    @Test
    public void testKafkaPartition() {
        KafkaPartition partition = new KafkaPartition("topic", 0);
        Assert.assertEquals(partition.getTopic(), "topic");
        Assert.assertEquals(partition.getPartition(), 0);
        Assert.assertEquals(partition.getName(), "topic-0");

        KafkaPartition partition2 = new KafkaPartition("topic", 0);
        Assert.assertEquals(partition.hashCode(), partition2.hashCode());
        Assert.assertEquals(partition, partition);
        Assert.assertEquals(partition, partition2);
        Assert.assertNotEquals(partition, null);
    }

    @Test
    public void testKafkaOffset() {
        KafkaOffset offset = new KafkaOffset(100, 11111111);
        Assert.assertEquals(offset.getKafkaOffset(), 100L);
        Assert.assertEquals(offset.humanReadable(), "1970-01-01 11:05:11");
    }

    @Test
    public void testConsoleOffset() throws IOException {
        KafkaOffset test = new KafkaOffset(111L, 11111111L);
        Map<String, String> kvMap = JSON.parseObject(new ConsoleOffset(test).toJson(), Map.class);
        Assert.assertEquals(kvMap.get("offset"), "11111111");
        Assert.assertEquals(kvMap.get("type"), "TIMESTAMP");
        Assert.assertTrue(Long.parseLong(kvMap.get("writeTime")) > 0L);
    }
}
