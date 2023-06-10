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

package com.antgroup.geaflow.console.biz.shared.convert;

import com.antgroup.geaflow.console.biz.shared.view.ReleaseView;
import com.antgroup.geaflow.console.core.model.GeaflowName;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ReleaseViewConverter extends IdViewConverter<GeaflowRelease, ReleaseView> {

    @Autowired
    private JobViewConverter jobViewConverter;

    @Override
    protected ReleaseView modelToView(GeaflowRelease model) {
        ReleaseView releaseView = super.modelToView(model);
        releaseView.setClusterName(Optional.ofNullable(model.getCluster()).map(GeaflowName::getName).orElse(null));
        releaseView.setVersionName(Optional.ofNullable(model.getVersion()).map(GeaflowName::getName).orElse(null));
        releaseView.setJob(jobViewConverter.convert(model.getJob()));
        releaseView.setJobConfig(model.getJobConfig());
        releaseView.setClusterConfig(model.getClusterConfig());
        releaseView.setJobPlan(model.getJobPlan());
        releaseView.setReleaseVersion(model.getReleaseVersion());
        return releaseView;
    }


}
