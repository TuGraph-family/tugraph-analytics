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

package com.antgroup.geaflow.common.metric;

import java.io.Serializable;

public class EventMetrics implements Serializable {

    /**
     * execute compute event time cost.
     */
    private long executeTime;

    /**
     * gc time in total during current task execution.
     */
    private long gcTime;

    /**
     * finish time of process certain windowId.
     */
    private long finishTime;

    /**
     * total input records.
     */
    private long inputRecords;
    private long inputBytes;

    /**
     * total output records.
     */
    private long outputRecords;
    private long outputBytes;

    private transient long startTs;

    private transient long startGcTs;

    public EventMetrics() {
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public long getGcTime() {
        return gcTime;
    }

    public void setGcTime(long gcTime) {
        this.gcTime = gcTime;
    }

    public long getStartTs() {
        return startTs;
    }

    public void setStartTs(long startTs) {
        this.startTs = startTs;
    }

    public long getStartGcTs() {
        return startGcTs;
    }

    public void setStartGcTs(long startGcTs) {
        this.startGcTs = startGcTs;
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public long getOutputRecords() {
        return outputRecords;
    }

    public void setOutputRecords(long outputRecords) {
        this.outputRecords = outputRecords;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
    }

}
