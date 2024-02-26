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
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.stats.model.EventInfo;
import com.antgroup.geaflow.stats.model.EventLabel;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import com.antgroup.geaflow.stats.model.StatsMetricType;
import com.antgroup.geaflow.stats.sink.IStatsWriter;

public class EventCollector extends BaseStatsCollector {

    EventCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportEvent(ExceptionLevel severityLevel, EventLabel label, String message) {
        reportEvent(severityLevel, label.name(), message);
    }

    public void reportEvent(ExceptionLevel severityLevel, String label, String message) {
        EventInfo eventInfo = new EventInfo(ProcessUtil.getHostname(),
            ProcessUtil.getHostIp(), ProcessUtil.getProcessId(),
            message, severityLevel.name(), label);
        addToWriterQueue(genEventKey(), eventInfo);
    }

    private String genEventKey() {
        return jobName + StatsMetricType.Event.getValue() + (Long.MAX_VALUE - System.currentTimeMillis());
    }

}
