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

package com.antgroup.geaflow.cluster.local.entrypoint;

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.local.clustermanager.LocalClusterManager;
import com.antgroup.geaflow.cluster.master.AbstractMaster;
import com.antgroup.geaflow.cluster.master.MasterContext;
import com.antgroup.geaflow.cluster.master.MasterFactory;
import com.antgroup.geaflow.common.config.Configuration;

public class LocalMasterRunner {

    private final AbstractMaster master;

    public LocalMasterRunner(Configuration configuration) {
        master = MasterFactory.create(configuration);
        MasterContext context = new MasterContext(configuration);
        context.setClusterManager(new LocalClusterManager());
        context.load();
        master.init(context);
    }

    public ClusterInfo init() {
        return master.startCluster();
    }

}
