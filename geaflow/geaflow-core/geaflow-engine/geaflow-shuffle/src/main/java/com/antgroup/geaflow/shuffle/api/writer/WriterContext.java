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

package com.antgroup.geaflow.shuffle.api.writer;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;

public class WriterContext implements IWriterContext {

    private final PipelineInfo pipelineInfo;
    private int edgeId;
    private int vertexId;
    private int taskIndex;
    private int taskId;
    private String taskName;
    private int targetChannels;
    private Configuration config;
    private ShuffleDescriptor descriptor;
    private int refCount;
    private IEncoder<?> encoder;

    public WriterContext(long pipelineId, String pipelineName) {
        this.pipelineInfo = new PipelineInfo(pipelineId, pipelineName);
    }

    public WriterContext setEdgeId(int edgeId) {
        this.edgeId = edgeId;
        return this;
    }

    public WriterContext setVertexId(int vertexId) {
        this.vertexId = vertexId;
        return this;
    }

    public WriterContext setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
        return this;
    }

    public WriterContext setTaskId(int taskId) {
        this.taskId = taskId;
        return this;
    }

    public WriterContext setTaskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    public WriterContext setChannelNum(int targetChannels) {
        this.targetChannels = targetChannels;
        return this;
    }

    public WriterContext setConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public WriterContext setShuffleDescriptor(ShuffleDescriptor descriptor) {
        this.descriptor = descriptor;
        return this;
    }

    public WriterContext setEncoder(IEncoder<?> encoder) {
        this.encoder = encoder;
        return this;
    }

    @Override
    public PipelineInfo getPipelineInfo() {
        return pipelineInfo;
    }

    @Override
    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public int getVertexId() {
        return vertexId;
    }

    @Override
    public int getTaskId() {
        return taskId;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public int getTaskIndex() {
        return taskIndex;
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    @Override
    public int getTargetChannelNum() {
        return targetChannels;
    }

    @Override
    public ShuffleDescriptor getShuffleDescriptor() {
        return descriptor;
    }

    @Override
    public IEncoder<?> getEncoder() {
        return this.encoder;
    }

    public static WriterContextBuilder newBuilder() {
        return new WriterContextBuilder();
    }

    public static class WriterContextBuilder {

        private long pipelineId;

        public WriterContextBuilder setPipelineId(long pipelineId) {
            this.pipelineId = pipelineId;
            return this;
        }

        public WriterContext setPipelineName(String pipelineName) {
            return new WriterContext(pipelineId, pipelineName);
        }
    }

}
