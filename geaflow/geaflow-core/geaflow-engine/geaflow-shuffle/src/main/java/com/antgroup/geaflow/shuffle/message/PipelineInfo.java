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

package com.antgroup.geaflow.shuffle.message;

import java.io.Serializable;

public class PipelineInfo implements Serializable {

    private final long pipelineId;
    private final String pipelineName;

    public PipelineInfo(long pipelineId, String pipelineName) {
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    @Override
    public String toString() {
        return "PipelineInfo{" + "pipelineId=" + pipelineId + ", pipelineName='" + pipelineName + '\'' + '}';
    }
}
