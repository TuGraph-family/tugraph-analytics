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

import com.antgroup.geaflow.console.common.service.integration.engine.CompileResult;
import com.antgroup.geaflow.console.core.model.code.GeaflowCode;
import com.antgroup.geaflow.console.core.model.job.GeaflowJob;
import com.antgroup.geaflow.console.core.model.job.GeaflowTransferJob;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.release.JobPlan;
import com.antgroup.geaflow.console.core.model.release.JobPlanBuilder;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import com.antgroup.geaflow.console.core.service.JobService;
import com.antgroup.geaflow.console.core.service.ReleaseService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GenerateJobPlanStage extends GeaflowBuildStage {

    @Autowired
    private ReleaseService releaseService;

    @Autowired
    private JobService jobService;

    public void init(GeaflowRelease release) {
        // Hla Jobs don't need compile
        GeaflowJob job = release.getJob();
        if (job.isApiJob()) {
            return;
        }
        // Generate code for transferJobs
        generateCode(job);
        GeaflowVersion version = release.getVersion();
        release.setJobPlan(compileJobPlan(job, version, null));
    }

    @Override
    public boolean update(GeaflowRelease release, ReleaseUpdate update) {
        GeaflowJob job = release.getJob();
        if (job.isApiJob()) {
            return false;
        }

        Map<String, Integer> newParallelisms = update.getNewParallelisms();
        if (newParallelisms == null) {
            return false;
        }

        JobPlanBuilder.setParallelisms(release.getJobPlan(), newParallelisms);

        generateCode(job);

        GeaflowVersion version = release.getVersion();
        JobPlan newJobPlan = compileJobPlan(job, version, newParallelisms);
        release.setJobPlan(newJobPlan);

        return true;
    }

    private JobPlan compileJobPlan(GeaflowJob job, GeaflowVersion version, Map<String, Integer> parallelisms) {
        CompileResult compileResult = releaseService.compile(job, version, parallelisms);
        return JobPlanBuilder.build(compileResult.getPhysicPlan());
    }

    private void generateCode(GeaflowJob job) {
        if (job instanceof GeaflowTransferJob) {
            GeaflowCode geaflowCode = ((GeaflowTransferJob) job).generateCode();
            ((GeaflowTransferJob) job).setUserCode(geaflowCode.getText());
            jobService.update(job);
        }
    }
}
