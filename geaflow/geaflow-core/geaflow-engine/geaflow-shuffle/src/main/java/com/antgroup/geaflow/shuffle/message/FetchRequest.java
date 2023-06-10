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

import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class FetchRequest implements Serializable {

    private long pipelineId;
    private String pipelineName;
    private int vertexId;
    private int taskId;
    private int taskIndex;
    private String taskName;
    private long targetBatchId;
    // Map<edgeId, sliceList> is upstream input partition result mapping info.
    private Map<Integer, List<ISliceMeta>> inputSlices;
    // Map<edgeId, streamName> is upstream input partition result mapping info.
    private Map<Integer, String> inputStreamMap;
    private Map<Integer, IEncoder<?>> encoders;
    private ShuffleDescriptor descriptor;

    public long getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public long getTargetBatchId() {
        return targetBatchId;
    }

    public void setTargetBatchId(long targetBatchId) {
        this.targetBatchId = targetBatchId;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
    }

    public int getVertexId() {
        return vertexId;
    }

    public void setVertexId(int vertexId) {
        this.vertexId = vertexId;
    }

    public Map<Integer, List<ISliceMeta>> getInputSlices() {
        return inputSlices;
    }

    public void setInputSlices(Map<Integer, List<ISliceMeta>> inputSlices) {
        this.inputSlices = inputSlices;
    }

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setInputStreamMap(Map<Integer, String> inputStreamMap) {
        this.inputStreamMap = inputStreamMap;
    }

    public Map<Integer, String> getInputStreamMap() {
        return inputStreamMap;
    }

    public Map<Integer, IEncoder<?>> getEncoders() {
        return this.encoders;
    }

    public void setEncoders(Map<Integer, IEncoder<?>> encoders) {
        this.encoders = encoders;
    }

    public ShuffleDescriptor getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(ShuffleDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public String toString() {
        return "FetchRequest{"
            + "pipelineId=" + pipelineId
            + ", pipelineName=" + pipelineName
            + ", vertexId=" + vertexId
            + ", taskId=" + taskId
            + ", taskIndex=" + taskIndex
            + ", targetBatchId=" + targetBatchId
            + ", inputSlices=" + inputSlices
            + ", inputStreamMap=" + inputStreamMap
            + '}';
    }

}
