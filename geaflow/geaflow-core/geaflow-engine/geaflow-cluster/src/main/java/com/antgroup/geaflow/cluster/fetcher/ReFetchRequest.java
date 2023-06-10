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

package com.antgroup.geaflow.cluster.fetcher;

public class ReFetchRequest implements IFetchRequest {

    private long startBatchId;
    private long windowCount;

    public ReFetchRequest(long startBatchId, long windowCount) {
        this.startBatchId = startBatchId;
        this.windowCount = windowCount;
    }

    public long getStartBatchId() {
        return startBatchId;
    }

    public void setStartBatchId(long startBatchId) {
        this.startBatchId = startBatchId;
    }

    public long getWindowCount() {
        return windowCount;
    }

    public void setWindowCount(int windowCount) {
        this.windowCount = windowCount;
    }
}
