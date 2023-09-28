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

import com.antgroup.geaflow.cluster.shuffle.LogicalPipelineSliceMeta;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InitFetchRequest implements IFetchRequest {

    private final long pipelineId;
    private final String pipelineName;

    private int taskId;
    private int taskIndex;
    private String taskName;
    private int vertexId;

    private ShuffleDescriptor descriptor;
    private List<IInputMessageBuffer<?>> fetchListeners;
    private Map<Integer, String> inputStreamMap;
    private Map<Integer, List<ISliceMeta>> inputSlices;
    private Map<Integer, IEncoder<?>> encoders;
    private int totalSliceNum;

    public InitFetchRequest(long pipelineId, String pipelineName) {
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.fetchListeners = new ArrayList<>();
    }

    public long getPipelineId() {
        return pipelineId;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getVertexId() {
        return vertexId;
    }

    public Map<Integer, String> getInputStreamMap() {
        return inputStreamMap;
    }

    public ShuffleDescriptor getDescriptor() {
        return descriptor;
    }

    public List<IInputMessageBuffer<?>> getFetchListeners() {
        return fetchListeners;
    }

    public Map<Integer, List<ISliceMeta>> getInputSlices() {
        return inputSlices;
    }

    public int getTotalSliceNum() {
        return totalSliceNum;
    }

    public void addListener(IInputMessageBuffer<?> listener) {
        fetchListeners.add(listener);
    }

    public InitFetchRequest setTaskId(int taskId) {
        this.taskId = taskId;
        return this;
    }

    public InitFetchRequest setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
        return this;
    }

    public InitFetchRequest setTaskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public InitFetchRequest setVertexId(int vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    public InitFetchRequest setDescriptor(ShuffleDescriptor descriptor) {
        this.descriptor = descriptor;
        return this;
    }

    public InitFetchRequest setInputStreamMap(Map<Integer, String> inputStreamMap) {
        this.inputStreamMap = inputStreamMap;
        return this;
    }

    public void setInputShardMap(Map<Integer, List<Shard>> inputShardMap) {
        if (inputShardMap != null) {
            inputSlices = new HashMap<>();
            for (Map.Entry<Integer, List<Shard>> entry : inputShardMap.entrySet()) {
                List<ISliceMeta> slices = new ArrayList<>();
                for (Shard shard : entry.getValue()) {
                    for (ISliceMeta sliceMeta : shard.getSlices()) {
                        if (sliceMeta instanceof LogicalPipelineSliceMeta) {
                            // convert to physical shuffle slice meta
                            LogicalPipelineSliceMeta logicalPipelineSliceMeta = (LogicalPipelineSliceMeta) sliceMeta;
                            slices.add(logicalPipelineSliceMeta.toPhysicalPipelineSliceMeta());
                        } else {
                            slices.add(sliceMeta);
                        }
                    }
                }
                inputSlices.put(entry.getKey(), slices);
                totalSliceNum += slices.size();
            }
        }
    }

    public Map<Integer, IEncoder<?>> getEncoders() {
        return this.encoders;
    }

    public InitFetchRequest setEncoders(Map<Integer, IEncoder<?>> encoders) {
        this.encoders = encoders;
        return this;
    }

    public static class InitRequestBuilder {

        private long pipelineId;
        private String pipelineName;

        public InitRequestBuilder(long pipelineId, String pipelineName) {
            this.pipelineId = pipelineId;
            this.pipelineName = pipelineName;
        }

        public InitFetchRequest setInputShardMap(Map<Integer, List<Shard>> inputShardMap) {
            InitFetchRequest request = new InitFetchRequest(pipelineId, pipelineName);
            request.setInputShardMap(inputShardMap);
            return request;
        }
    }
}
