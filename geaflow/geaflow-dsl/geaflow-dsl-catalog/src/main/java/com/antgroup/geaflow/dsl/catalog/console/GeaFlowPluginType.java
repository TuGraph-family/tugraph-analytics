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

package com.antgroup.geaflow.dsl.catalog.console;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

/**
 * Plugin type supported on the console platform.
 */
public enum GeaFlowPluginType {
    /**
     * Kafka plugin type for table.
     */
    KAFKA,
    /**
     * Hive plugin type for table.
     */
    HIVE,
    /**
     * File plugin type for table.
     */
    FILE,
    /**
     * Memory plugin type for graph.
     */
    MEMORY,
    /**
     * Rocksdb plugin type for graph.
     */
    ROCKSDB,
    /**
     * Socket plugin type for table.
     */
    SOCKET,
    /**
     * Console plugin type for table.
     */
    CONSOLE;

    public static GeaFlowPluginType getPluginType(String name) {
        for (GeaFlowPluginType type : values()) {
            if (type.name().equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new GeaflowRuntimeException("can not find relate plugin type: " + name);
    }
}
