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

package com.antgroup.geaflow.shuffle.service;

import com.antgroup.geaflow.shuffle.api.reader.IShuffleReader;
import com.antgroup.geaflow.shuffle.api.writer.IShuffleWriter;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import java.io.Serializable;

public interface IShuffleService extends Serializable {

    /**
     * Init the shuffle service with job config.
     *
     * @param connectionManager connection manager.
     */
    void init(IConnectionManager connectionManager);

    /**
     * get shuffle writer per reducer task.
     *
     * @return shuffle reader.
     */
    IShuffleReader getReader();

    /**
     * get shuffle reader per mapper task.
     *
     * @return shuffle writer.
     */
    IShuffleWriter getWriter();

    /**
     * Release the local resources of this job id.
     */
    void clean(PipelineInfo jobInfo);
}
