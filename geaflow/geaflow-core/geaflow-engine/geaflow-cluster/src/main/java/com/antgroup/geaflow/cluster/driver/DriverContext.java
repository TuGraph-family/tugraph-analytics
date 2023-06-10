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

package com.antgroup.geaflow.cluster.driver;

import com.antgroup.geaflow.cluster.common.IReliableContext;
import com.antgroup.geaflow.cluster.common.ReliableContainerContext;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.Pipeline;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverContext extends ReliableContainerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    private Pipeline pipeline;
    private List<Integer> finishedPipelineTasks;

    public DriverContext(int index, Configuration config) {
        super(index, config);
        this.finishedPipelineTasks = new ArrayList<>();
    }

    public DriverContext(int index, Configuration config, boolean isRecover) {
        super(index, config);
        this.isRecover = isRecover;
        this.finishedPipelineTasks = new ArrayList<>();
    }

    @Override
    public void load() {
        Pipeline pipeline = ClusterMetaStore.getInstance(id, config).getPipeline();
        if (pipeline != null) {
            List<Integer> finishedPipelineTasks = ClusterMetaStore.getInstance().getPipelineTasks();
            if (finishedPipelineTasks == null) {
                finishedPipelineTasks = new ArrayList<>();
            }
            this.pipeline = pipeline;
            this.finishedPipelineTasks = finishedPipelineTasks;
            LOGGER.info("driver {} recover context {}", id, this);
        }
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void addPipeline(Pipeline pipeline) {
        if (!pipeline.equals(this.pipeline)) {
            this.pipeline = pipeline;
        }
    }

    public List<Integer> getFinishedPipelineTasks() {
        return finishedPipelineTasks;
    }

    public void addFinishedPipelineTask(int pipelineTaskIndex) {
        if (!finishedPipelineTasks.contains(pipelineTaskIndex)) {
            finishedPipelineTasks.add(pipelineTaskIndex);
        }
    }

    public static class PipelineCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            DriverContext driverContext = ((DriverContext) context);
            if (driverContext.getPipeline() != null) {
                ClusterMetaStore.getInstance().savePipeline(driverContext.getPipeline()).flush();
                LOGGER.info("driver {} checkpoint pipeline", driverContext.getId());
            }
        }
    }

    public static class PipelineTaskCheckpointFunction implements IReliableContextCheckpointFunction {

        @Override
        public void doCheckpoint(IReliableContext context) {
            DriverContext driverContext = ((DriverContext) context);
            if (driverContext.getFinishedPipelineTasks() != null && !driverContext.getFinishedPipelineTasks().isEmpty()) {
                ClusterMetaStore.getInstance().savePipelineTasks(driverContext.getFinishedPipelineTasks()).flush();
                LOGGER.info("driver {} checkpoint pipeline finished tasks {}",
                    driverContext.getId(), driverContext.getFinishedPipelineTasks());
            }
        }
    }
}
