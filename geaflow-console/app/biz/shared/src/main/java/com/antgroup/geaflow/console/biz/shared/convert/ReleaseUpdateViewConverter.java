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

import com.antgroup.geaflow.console.biz.shared.view.ReleaseUpdateView;
import com.antgroup.geaflow.console.core.model.cluster.GeaflowCluster;
import com.antgroup.geaflow.console.core.model.release.ReleaseUpdate;
import com.antgroup.geaflow.console.core.model.version.GeaflowVersion;
import org.springframework.stereotype.Component;

@Component
public class ReleaseUpdateViewConverter {

    public ReleaseUpdate converter(ReleaseUpdateView view, GeaflowVersion version, GeaflowCluster cluster) {
        ReleaseUpdate model = new ReleaseUpdate();
        model.setNewJobConfig(view.getNewJobConfig());
        model.setNewClusterConfig(view.getNewClusterConfig());
        model.setNewParallelisms(view.getNewParallelisms());
        model.setNewVersion(version);
        model.setNewCluster(cluster);
        return model;
    }
}
