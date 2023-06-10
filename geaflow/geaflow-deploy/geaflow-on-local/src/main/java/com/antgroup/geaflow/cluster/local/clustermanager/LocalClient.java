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

package com.antgroup.geaflow.cluster.local.clustermanager;

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.cluster.container.ContainerContext;
import com.antgroup.geaflow.cluster.driver.DriverContext;
import com.antgroup.geaflow.cluster.local.entrypoint.LocalContainerRunner;
import com.antgroup.geaflow.cluster.local.entrypoint.LocalDriverRunner;
import com.antgroup.geaflow.cluster.local.entrypoint.LocalMasterRunner;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalClient implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalClient.class);

    public static LocalMasterRunner createMaster(ClusterConfig clusterConfig) {
        return new LocalMasterRunner(clusterConfig.getConfig());
    }

    public static ClusterInfo initMaster(LocalMasterRunner master) {
        LOGGER.info("init master");
        return master.init();
    }

    public static LocalDriverRunner createDriver(ClusterConfig clusterConfig,
                                                              DriverContext context) {
        return new LocalDriverRunner(context);
    }

    public static LocalContainerRunner createContainer(ClusterConfig clusterConfig,
                                                                    ContainerContext containerContext) {
        return new LocalContainerRunner(containerContext);
    }

}
