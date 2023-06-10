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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.shuffle.message.PipelineInfo;
import com.antgroup.geaflow.shuffle.message.ShuffleId;

public interface IShuffleMaster {

    /**
     * Init shuffle meta.
     *
     * @param jobConfig configuration.
     */
    void init(Configuration jobConfig);

    /**
     * Clean all shuffle spill/meta/data related to this shuffle Id.
     *
     * @param shuffleId shuffle id.
     */
    void clean(ShuffleId shuffleId);

    /**
     * Clean all shuffle spill/meta/data related to this job.
     *
     * @param jobInfo pipeline info.
     */
    void clean(PipelineInfo jobInfo);

    /**
     * Release shuffle data.
     */
    void close();

}
