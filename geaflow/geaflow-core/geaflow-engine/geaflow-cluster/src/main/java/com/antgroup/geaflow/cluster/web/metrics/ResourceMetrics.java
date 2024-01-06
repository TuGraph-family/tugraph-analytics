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

package com.antgroup.geaflow.cluster.web.metrics;

import java.io.Serializable;

public class ResourceMetrics implements Serializable {

    private int totalWorkers;
    private int availableWorkers;
    private int pendingWorkers;

    public int getTotalWorkers() {
        return totalWorkers;
    }

    public void setTotalWorkers(int totalWorkers) {
        this.totalWorkers = totalWorkers;
    }

    public int getAvailableWorkers() {
        return availableWorkers;
    }

    public void setAvailableWorkers(int availableWorkers) {
        this.availableWorkers = availableWorkers;
    }

    public int getPendingWorkers() {
        return pendingWorkers;
    }

    public void setPendingWorkers(int pendingWorkers) {
        this.pendingWorkers = pendingWorkers;
    }

}
