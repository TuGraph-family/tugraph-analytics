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

package com.antgroup.geaflow.console.core.model.job.config;

import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ClusterConfigClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.container.num", comment = "Container数")
    @GeaflowConfigValue(required = true)
    private Integer containers;

    @GeaflowConfigKey(value = "geaflow.container.worker.num", comment = "Container内worker数")
    @GeaflowConfigValue(required = true)
    private Integer containerWorkers;

    @GeaflowConfigKey(value = "geaflow.container.memory.mb", comment = "Container内存(MB)")
    @GeaflowConfigValue(required = true, defaultValue = "256")
    private Integer containerMemory;

    @GeaflowConfigKey(value = "geaflow.container.vcores", comment = "Container核数(vcore)")
    @GeaflowConfigValue(required = true, defaultValue = "1")
    private Double containerCores;

    @GeaflowConfigKey(value = "geaflow.container.jvm.options", comment = "Container JVM参数")
    @GeaflowConfigValue(required = true)
    private String containerJvmOptions;

    @GeaflowConfigKey(value = "geaflow.fo.enable", comment = "是否开启FO")
    @GeaflowConfigValue(defaultValue = "true")
    private Boolean enableFo;

    @GeaflowConfigKey(value = "geaflow.client.memory.mb", comment = "Client内存(MB)")
    @GeaflowConfigValue(defaultValue = "1024")
    private Integer clientMemory;

    @GeaflowConfigKey(value = "geaflow.master.memory.mb", comment = "Master内存(MB)")
    @GeaflowConfigValue(defaultValue = "4096")
    private Integer masterMemory;

    @GeaflowConfigKey(value = "geaflow.driver.memory.mb", comment = "Driver内存(MB)")
    @GeaflowConfigValue(defaultValue = "4096")
    private Integer driverMemory;

    @GeaflowConfigKey(value = "geaflow.client.vcores", comment = "Client核数(vcore)")
    @GeaflowConfigValue(defaultValue = "1")
    private Double clientCores;

    @GeaflowConfigKey(value = "geaflow.master.vcores", comment = "Master核数(vcore)")
    @GeaflowConfigValue(defaultValue = "1")
    private Double masterCores;

    @GeaflowConfigKey(value = "geaflow.driver.vcores", comment = "Driver核数(vcore)")
    @GeaflowConfigValue(defaultValue = "1")
    private Double driverCores;

    @GeaflowConfigKey(value = "geaflow.client.jvm.options", comment = "Client JVM参数")
    @GeaflowConfigValue(defaultValue = "-Xmx1024m,-Xms1024m,-Xmn256m,-Xss256k,-XX:MaxDirectMemorySize=512m")
    private String clientJvmOptions;

    @GeaflowConfigKey(value = "geaflow.master.jvm.options", comment = "Master JVM参数")
    @GeaflowConfigValue(defaultValue = "-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m")
    private String masterJvmOptions;

    @GeaflowConfigKey(value = "geaflow.driver.jvm.options", comment = "Driver JVM参数")
    @GeaflowConfigValue(defaultValue = "-Xmx2048m,-Xms2048m,-Xmn512m,-Xss512k,-XX:MaxDirectMemorySize=1024m")
    private String driverJvmOptions;

}
