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

package com.antgroup.geaflow.env.ctx;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import java.util.Map;
import java.util.UUID;

public class EnvironmentContext implements IEnvironmentContext {

    private final Configuration config;

    public EnvironmentContext() {
        this.config = new Configuration();
        buildDefaultConfig();
    }

    @Override
    public void withConfig(Map<String, String> config) {
        if (config != null) {
            this.config.putAll(config);
        }
    }

    public Configuration getConfig() {
        return config;
    }

    private void buildDefaultConfig() {
        String jobUid = UUID.randomUUID().toString();
        this.config.put(ExecutionConfigKeys.JOB_UNIQUE_ID, jobUid);
        this.config.put(ExecutionConfigKeys.JOB_APP_NAME, "geaflow" + jobUid);
    }

}
