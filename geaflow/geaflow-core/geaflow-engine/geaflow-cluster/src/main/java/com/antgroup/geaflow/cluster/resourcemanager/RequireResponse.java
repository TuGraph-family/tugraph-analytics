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

public class RequireResponse {

    private final String requireId;
    private final boolean success;
    private final List<WorkerInfo> workers;
    private final String msg;

    private RequireResponse(String requireId, boolean success, List<WorkerInfo> workers, String msg) {
        this.requireId = requireId;
        this.success = success;
        this.workers = workers;
        this.msg = msg;
    }

    public String getRequireId() {
        return this.requireId;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public List<WorkerInfo> getWorkers() {
        return this.workers;
    }

    public String getMsg() {
        return this.msg;
    }

    public static RequireResponse success(String requireId, List<WorkerInfo> workers) {
        return new RequireResponse(requireId, true, workers, null);
    }

    public static RequireResponse fail(String requireId, String msg) {
        return new RequireResponse(requireId, false, null, msg);
    }

}
