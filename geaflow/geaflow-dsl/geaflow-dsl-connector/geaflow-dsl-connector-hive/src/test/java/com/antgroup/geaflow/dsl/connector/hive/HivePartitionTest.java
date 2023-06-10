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

package com.antgroup.geaflow.dsl.connector.hive;

import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.hive.HiveTableSource.HiveOffset;
import com.antgroup.geaflow.dsl.connector.hive.HiveTableSource.HivePartition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HivePartitionTest {

    @Test
    public void testHivePartition() {
        Partition p1 = new HivePartition(
            "default", "testTable1", null, null, null, new String[0]);
        Partition p2 = new HivePartition(
            "default", "testTable2", null, null, null, new String[0]);
        Partition _p1 = new HivePartition(
            "default", "testTable1", null, null, null, new String[0]);
        Assert.assertEquals(p1.hashCode(), _p1.hashCode());
        Assert.assertEquals(p1, _p1);
        Assert.assertNotEquals(p1.hashCode(), p2.hashCode());
        Assert.assertNotEquals(p1, p2);
    }

    @Test
    public void testHiveOffset() {
        HiveOffset test = new HiveOffset(0L);
        Assert.assertEquals(test.humanReadable(), "0");
        Assert.assertEquals(test.getOffset(), 0L);
        Assert.assertEquals(test.isTimestamp(), false);
    }
}
