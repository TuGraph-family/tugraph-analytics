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

package com.antgroup.geaflow.pipeline.service;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.pipeline.context.IPipelineExecutorContext;

public interface IPipelineServiceExecutorContext extends IPipelineExecutorContext {

    /**
     * Returns the config of pipeline service executor context.
     */
    Configuration getConfiguration();

    /**
     * Returns the pipeline service which need be started.
     */
    PipelineService getPipelineService();
}
