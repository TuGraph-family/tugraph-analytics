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
import com.antgroup.geaflow.stats.model.StatsStoreType;

public class StatsWriterFactory {

    public static IStatsWriter getStatsWriter(Configuration configuration) {
        return getStatsWriter(configuration, false);
    }

    public static IStatsWriter getStatsWriter(Configuration configuration, boolean isSync) {
        String statStoreType = configuration.getString(ExecutionConfigKeys.STATS_METRIC_STORE_TYPE);
        if (statStoreType.equalsIgnoreCase(StatsStoreType.MEMORY.name())) {
            return new MemoryStatsWriter();
        } else if (statStoreType.equalsIgnoreCase(StatsStoreType.HBASE.name()) || statStoreType
            .equalsIgnoreCase(StatsStoreType.JDBC.name())) {
            return isSync ? new SyncKvStoreWriter(configuration) :
                   new AsyncKvStoreWriter(configuration);
        }
        throw new UnsupportedOperationException("unknown stats store type" + statStoreType);
    }
}
