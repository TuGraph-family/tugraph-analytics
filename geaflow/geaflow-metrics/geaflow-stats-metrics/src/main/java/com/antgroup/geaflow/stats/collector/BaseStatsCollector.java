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

package com.antgroup.geaflow.stats.collector;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.stats.sink.IStatsWriter;
import java.util.ArrayList;
import java.util.List;

public class BaseStatsCollector {

    protected String jobName;
    private final List<IStatsWriter> statsWriters;

    public BaseStatsCollector(IStatsWriter statsWriter, Configuration configuration) {
        this.statsWriters = new ArrayList<>();
        this.statsWriters.add(statsWriter);
        this.jobName = configuration.getString(ExecutionConfigKeys.JOB_APP_NAME);
    }

    public void addToWriterQueue(String key, Object value) {
        for (int i = 0; i < statsWriters.size(); i++) {
            statsWriters.get(i).addMetric(key, value);
        }
    }

}
