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

package com.antgroup.geaflow.common.heartbeat;

import com.antgroup.geaflow.common.metric.ProcessMetrics;
import java.io.Serializable;

public class Heartbeat implements Serializable {

    private int containerId;
    private long timestamp;
    private String namePrefix;
    private ProcessMetrics processMetrics;

    public Heartbeat(int resourceId) {
        this.containerId = resourceId;
        this.timestamp = System.currentTimeMillis();
    }

    public int getContainerId() {
        return containerId;
    }

    public void setContainerId(int containerId) {
        this.containerId = containerId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ProcessMetrics getProcessMetrics() {
        return processMetrics;
    }

    public void setProcessMetrics(ProcessMetrics processMetrics) {
        this.processMetrics = processMetrics;
    }

    public String getNamePrefix() {
        return namePrefix;
    }

    public void setNamePrefix(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public String toString() {
        return "Heartbeat{" + "containerId=" + containerId + ", timestamp=" + timestamp
            + ", processMetrics=" + processMetrics + '}';
    }
}
