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

package com.antgroup.geaflow.cluster.constants;

public class ClusterConstants {

    private static final String DRIVER_PREFIX = "driver-";
    private static final String CONTAINER_PREFIX = "container-";
    public static final String PORT_SEPARATOR = ":";
    public static final String CLUSTER_TYPE = "clusterType";
    public static final String LOCAL_CLUSTER = "LOCAL";
    public static final int DEFAULT_MASTER_ID = 0;
    public static final int EXIT_CODE = -1;

    public static String getDriverName(int id) {
        return String.format("%s%s", DRIVER_PREFIX, id);
    }

    public static String getContainerName(int id) {
        return String.format("%s%s", CONTAINER_PREFIX, id);
    }

}
