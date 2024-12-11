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

package com.antgroup.geaflow.shuffle.api.reader;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.desc.ShardInputDesc;
import com.antgroup.geaflow.shuffle.message.PipelineSliceMeta;
import java.util.List;
import java.util.Map;

public class ReaderContext implements IReaderContext {

    private Configuration config;
    private int vertexId;
    private String taskName;
    private Map<Integer, ShardInputDesc> inputShardMap;
    private Map<Integer, List<PipelineSliceMeta>> inputSlices;
    private int sliceNum;

    @Override
    public Configuration getConfig() {
        return this.config;
    }

    public int getVertexId() {
        return this.vertexId;
    }

    public String getTaskName() {
        return this.taskName;
    }

    public Map<Integer, ShardInputDesc> getInputShardMap() {
        return this.inputShardMap;
    }

    public Map<Integer, List<PipelineSliceMeta>> getInputSlices() {
        return this.inputSlices;
    }

    public int getSliceNum() {
        return this.sliceNum;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setInputShardMap(Map<Integer, ShardInputDesc> inputShardMap) {
        this.inputShardMap = inputShardMap;
    }

    public void setInputSlices(Map<Integer, List<PipelineSliceMeta>> inputSlices) {
        this.inputSlices = inputSlices;
    }

    public void setSliceNum(int sliceNum) {
        this.sliceNum = sliceNum;
    }

}