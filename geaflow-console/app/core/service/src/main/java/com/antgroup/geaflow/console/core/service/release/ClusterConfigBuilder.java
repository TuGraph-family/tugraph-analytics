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
import com.antgroup.geaflow.console.core.model.job.config.ClusterConfigClass;
import com.antgroup.geaflow.console.core.model.job.config.ServeJobConfigClass;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ClusterConfigBuilder {

    private static ClusterConfigClass getDefaultClusterConfig() {
        ClusterConfigClass clusterConfig = new ClusterConfigClass();
        clusterConfig.setContainers(1);
        clusterConfig.setContainerWorkers(3);
        clusterConfig.setContainerMemory(1024);
        clusterConfig.setContainerCores(1.5);
        clusterConfig.setContainerJvmOptions("-Xmx512m,-Xms512m,-Xmn256m");

        clusterConfig.setClientMemory(1024);
        clusterConfig.setMasterMemory(1024);
        clusterConfig.setDriverMemory(1024);

        clusterConfig.setClientCores(0.5);
        clusterConfig.setMasterCores(1.0);
        clusterConfig.setDriverCores(1.0);

        clusterConfig.setClientJvmOptions("-Xmx512m,-Xms512m,-Xmn256m,-Xss512k,-XX:MaxDirectMemorySize=128m");
        clusterConfig.setMasterJvmOptions("-Xmx512m,-Xms512m,-Xmn256m,-Xss512k,-XX:MaxDirectMemorySize=128m");
        clusterConfig.setDriverJvmOptions("-Xmx512m,-Xms512m,-Xmn256m,-Xss512k,-XX:MaxDirectMemorySize=128m");

        return clusterConfig;
    }

    public static GeaflowConfig buildDefaultConfig(GeaflowRelease release) {
        ClusterConfigClass configClass = getDefaultClusterConfig();
        switch (release.getJob().getType()) {
            case SERVE:
                ServeJobConfigClass jobConfig = release.getJobConfig().parse(ServeJobConfigClass.class);
                Integer driverNum = jobConfig.getDriverNum();
                Integer queryParallelism = jobConfig.getQueryParallelism();
                configClass.setContainerWorkers(driverNum * queryParallelism + 1);
                configClass.setContainerMemory(1200);
                configClass.setContainerJvmOptions("-Xmx800m,-Xms800m,-Xmn256m");
                configClass.setDriverMemory(800);
                configClass.setDriverJvmOptions("-Xmx400m,-Xms400m,-Xmn150m,-Xss512k,-XX:MaxDirectMemorySize=128m");
                break;
            default:
                break;
        }

        return configClass.build();
    }

}
