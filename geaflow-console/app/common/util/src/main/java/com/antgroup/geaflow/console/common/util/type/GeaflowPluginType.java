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

package com.antgroup.geaflow.console.common.util.type;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum GeaflowPluginType {
    KAFKA,

    HIVE,

    FILE,

    SOCKET,

    CONSOLE,

    MEMORY,

    ROCKSDB,

    LOCAL,

    DFS,

    OSS,

    JDBC,

    REDIS,

    INFLUXDB,

    K8S,

    CONTAINER,

    RAY,

    /**
     * just for custom define or unknown type, not a specific type.
     */
    None;


    public static GeaflowPluginType of(String type) {
        try {
            return GeaflowPluginType.valueOf(type);
        } catch (Exception e) {
            return GeaflowPluginType.None;
        }
    }

    public static String getName(String type) {
        if (type == null) {
            return null;
        }
        GeaflowPluginType typeEnum = GeaflowPluginType.of(type.toUpperCase());
        return typeEnum == None ? type : typeEnum.name();
    }
}
