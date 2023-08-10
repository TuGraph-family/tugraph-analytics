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

import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GeaflowBuildPipeline {

    private static final List<GeaflowBuildStage> STAGES = new ArrayList<>();

    @Autowired
    private ResolveReleaseVersionStage resolveReleaseVersionStage;

    @Autowired
    private ResolveVersionStage resolveVersionStage;

    @Autowired
    private GenerateJobPlanStage generateJobPlanStage;

    @Autowired
    private GenerateJobConfigStage generateJobConfigStage;

    @Autowired
    private GenerateClusterConfigStage generateClusterConfigStage;

    @Autowired
    private ResolveClusterStage resolveClusterStage;

    @Autowired
    private PackageStage packageStage;

    @PostConstruct
    public void init() {
        STAGES.add(resolveReleaseVersionStage);
        STAGES.add(resolveVersionStage);
        STAGES.add(generateJobPlanStage);
        STAGES.add(generateJobConfigStage);
        STAGES.add(generateClusterConfigStage);
        STAGES.add(resolveClusterStage);
        STAGES.add(packageStage);
    }


    public static GeaflowRelease build(GeaflowJob job) {
        GeaflowRelease release = new GeaflowRelease();
        release.setJob(job);
        for (GeaflowBuildStage stage : STAGES) {
            stage.init(release);
        }
        return release;
    }

    public static void update(GeaflowRelease release, ReleaseUpdate update) {
        boolean initNext = true;
        for (GeaflowBuildStage stage : STAGES) {
            if (initNext) {
                stage.init(release);
            }
            initNext = stage.update(release, update);
        }
    }

}
