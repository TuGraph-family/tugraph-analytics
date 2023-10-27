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

package com.antgroup.geaflow.example.util;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.CLUSTER_TYPE;

import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.env.IEnvironment.EnvType;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtil.class);

    public static Environment loadEnvironment(String[] args) {
        EnvType clusterType = getClusterType();
        switch (clusterType) {
            case K8S:
                return EnvironmentFactory.onK8SEnvironment(args);
            case RAY_COMMUNITY:
                return EnvironmentFactory.onRayCommunityEnvironment(args);
            default:
                return EnvironmentFactory.onLocalEnvironment(args);
        }
    }

    private static EnvType getClusterType() {
        String clusterType = System.getProperty(CLUSTER_TYPE);
        if (StringUtils.isBlank(clusterType)) {
            LOGGER.warn("use local as default cluster");
            return EnvType.LOCAL;
        }
        return (EnvType.valueOf(clusterType.toUpperCase(Locale.ROOT)));
    }

}
