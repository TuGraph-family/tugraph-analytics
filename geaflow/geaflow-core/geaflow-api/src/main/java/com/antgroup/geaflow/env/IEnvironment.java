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

package com.antgroup.geaflow.env;

import com.antgroup.geaflow.pipeline.IPipelineResult;
import java.io.Serializable;

public interface IEnvironment extends Serializable {

    /**
     * Initialize environment.
     */
    void init();

    /**
     * Submit pipeline by geaflow client.
     */
    IPipelineResult submit();

    /**
     * Shutdown geaflow client.
     */
    void shutdown();

    /**
     * Returns the env type.
     */
    EnvType getEnvType();

    enum EnvType {

        /**
         * Community ray cluster.
         */
        RAY_COMMUNITY,

        /**
         * K8s cluster.
         */
        K8S,

        /**
         * Local.
         */
        LOCAL,
    }

}
