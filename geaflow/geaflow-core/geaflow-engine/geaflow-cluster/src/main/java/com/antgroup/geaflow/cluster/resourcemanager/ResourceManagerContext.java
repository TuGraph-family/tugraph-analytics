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

package com.antgroup.geaflow.cluster.resourcemanager;

import com.antgroup.geaflow.cluster.clustermanager.ClusterContext;
import com.antgroup.geaflow.cluster.master.MasterContext;
import com.antgroup.geaflow.common.config.Configuration;

public class ResourceManagerContext {

    private final Configuration config;
    private final ClusterContext clusterContext;
    private boolean recover;

    private ResourceManagerContext(MasterContext masterContext, ClusterContext clusterContext) {
        this.config = masterContext.getConfiguration();
        this.clusterContext = clusterContext;
        this.recover = masterContext.isRecover();
    }

    public Configuration getConfig() {
        return this.config;
    }

    public ClusterContext getClusterContext() {
        return this.clusterContext;
    }

    public boolean isRecover() {
        return this.recover;
    }

    public void setRecover(boolean recover) {
        this.recover = recover;
    }

    public static ResourceManagerContext build(MasterContext masterContext, ClusterContext clusterContext) {
        return new ResourceManagerContext(masterContext, clusterContext);
    }

}
