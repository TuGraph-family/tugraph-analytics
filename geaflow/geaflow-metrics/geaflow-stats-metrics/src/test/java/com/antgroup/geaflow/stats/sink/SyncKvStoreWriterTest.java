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

package com.antgroup.geaflow.stats.sink;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.stats.model.StatsStoreType;
import org.testng.annotations.Test;

public class SyncKvStoreWriterTest {

    @Test
    public void testSyncStatsWriter() {
        Configuration configuration = new Configuration();
        configuration.put(ExecutionConfigKeys.STATS_METRIC_STORE_TYPE, StatsStoreType.MEMORY.name());
        SyncKvStoreWriter writer = new SyncKvStoreWriter(configuration);
        for (int i = 0; i < 1500; i++) {
            writer.addMetric(String.valueOf(i), i);
        }
        SleepUtils.sleepSecond(3);
        writer.close();
    }

}
