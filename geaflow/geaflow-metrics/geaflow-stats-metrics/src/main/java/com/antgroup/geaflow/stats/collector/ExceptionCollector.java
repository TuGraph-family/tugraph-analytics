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
import com.antgroup.geaflow.stats.model.ExceptionInfo;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import com.antgroup.geaflow.stats.model.StatsMetricType;
import com.antgroup.geaflow.stats.sink.IStatsWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class ExceptionCollector extends BaseStatsCollector {

    ExceptionCollector(IStatsWriter statsWriter, Configuration configuration) {
        super(statsWriter, configuration);
    }

    public void reportException(Throwable e) {
        reportException(ExceptionLevel.ERROR, e);
    }

    public void reportException(ExceptionLevel severityLevel, Throwable e) {
        ExceptionInfo log = new ExceptionInfo(ProcessUtil.getHostname(),
            ProcessUtil.getHostIp(), ProcessUtil.getProcessId(),
            ExceptionUtils.getStackTrace(e), severityLevel.name());
        addToWriterQueue(genExceptionKey(), log);
    }

    private String genExceptionKey() {
        return jobName + StatsMetricType.Exception.getValue() + (Long.MAX_VALUE - System.currentTimeMillis());
    }

}
