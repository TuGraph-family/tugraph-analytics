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

package com.antgroup.geaflow.cluster.ray.entrypoint;

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.master.Master;
import com.antgroup.geaflow.cluster.master.MasterContext;
import com.antgroup.geaflow.cluster.ray.clustermanager.RayClusterManager;
import com.antgroup.geaflow.cluster.ray.utils.RaySystemFunc;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.env.IEnvironment.EnvType;

public class RayMasterRunner {

    private final Master master;

    public RayMasterRunner(Configuration configuration) {
        master = new Master();
        MasterContext context = new MasterContext(configuration);
        context.setRecover(RaySystemFunc.isRestarted());
        context.setClusterManager(new RayClusterManager());
        context.setEnvType(EnvType.RAY_COMMUNITY);
        master.init(context);
    }

    public ClusterInfo init() {
        return master.startCluster();
    }

}
