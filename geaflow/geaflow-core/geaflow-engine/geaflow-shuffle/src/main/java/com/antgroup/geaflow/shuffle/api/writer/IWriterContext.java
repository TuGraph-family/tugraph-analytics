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
import java.io.Serializable;

public interface IWriterContext extends Serializable {

    /**
     * Get the pipeline information.
     *
     * @return pipeline info.
     */
    PipelineInfo getPipelineInfo();

    /**
     * Get the vertex id in the DAG.
     *
     * @return vertex id.
     */
    int getVertexId();

    /**
     * Get the edge id in the DAG.
     *
     * @return edge id.
     */
    int getEdgeId();

    /**
     * Get the task index of the writer.
     *
     * @return task index.
     */
    int getTaskIndex();

    /**
     * Get the task id of the writer.
     *
     * @return task id.
     */
    int getTaskId();

    /**
     * Get task name of the writer.
     *
     * @return task name.
     */
    String getTaskName();

    /**
     * Get the target channel number of downstream.
     *
     * @return target channel number.
     */
    int getTargetChannelNum();

    /**
     * Get the configuration.
     *
     * @return configuration.
     */
    Configuration getConfig();

    /**
     * Get the shuffle descriptor of the writer.
     *
     * @return shuffle descriptor.
     */
    ShuffleDescriptor getShuffleDescriptor();

    /**
     * Get the encoder for serialize and deserialize data.
     *
     * @return data encoder.
     */
    IEncoder<?> getEncoder();

}
