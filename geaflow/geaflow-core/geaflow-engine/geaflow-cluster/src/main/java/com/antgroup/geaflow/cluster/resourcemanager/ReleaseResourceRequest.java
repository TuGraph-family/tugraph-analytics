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

package com.antgroup.geaflow.cluster.resourcemanager;

import java.util.List;

public class ReleaseResourceRequest {

    private final String releaseId;
    private final List<WorkerInfo> workers;

    private ReleaseResourceRequest(String releaseId, List<WorkerInfo> workers) {
        this.releaseId = releaseId;
        this.workers = workers;
    }

    public String getReleaseId() {
        return this.releaseId;
    }

    public List<WorkerInfo> getWorkers() {
        return this.workers;
    }

    public static ReleaseResourceRequest build(String releaseId, List<WorkerInfo> workers) {
        return new ReleaseResourceRequest(releaseId, workers);
    }

}
