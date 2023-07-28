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

package com.antgroup.geaflow.console.core.service.release;

import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import org.apache.commons.collections.MapUtils;
import org.springframework.stereotype.Component;

@Component
public class GenerateJobConfigStage extends GeaflowBuildStage {

    private GeaflowConfig getDefaultConfig() {
        GeaflowConfig geaflowConfig = new GeaflowConfig();
        geaflowConfig.put("geaflow.heartbeat.timeout.ms", "300000");

        return geaflowConfig;
    }

    @Override
    public void init(GeaflowRelease release) {
        // generate default job config from job plan
        GeaflowConfig jobConfig = getDefaultConfig();

        if (release.getJobConfig() != null) {
            jobConfig.putAll(release.getJobConfig());
        }

        release.setJobConfig(jobConfig);

    }


    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        GeaflowConfig newJobConfig = update.getNewJobConfig();
        if (MapUtils.isEmpty(newJobConfig)) {
            return false;
        }
        release.setJobConfig(newJobConfig);
        return true;
    }
}
