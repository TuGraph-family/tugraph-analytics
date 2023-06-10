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

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public enum GeaflowFieldCategory {

    PROPERTY(GeaflowStructType.values()),

    ID(GeaflowStructType.TABLE, GeaflowStructType.VIEW),

    VERTEX_ID(GeaflowStructType.VERTEX),

    VERTEX_LABEL(GeaflowStructType.VERTEX),

    EDGE_SOURCE_ID(GeaflowStructType.EDGE),

    EDGE_TARGET_ID(GeaflowStructType.EDGE),

    EDGE_LABEL(GeaflowStructType.EDGE),

    EDGE_TIMESTAMP(GeaflowStructType.EDGE);

    private final Set<GeaflowStructType> structTypes;

    GeaflowFieldCategory(GeaflowStructType... structTypes) {
        this.structTypes = Sets.newHashSet(structTypes);
    }

    public static List<GeaflowFieldCategory> of(GeaflowStructType structType) {
        List<GeaflowFieldCategory> constraints = new ArrayList<>();
        for (GeaflowFieldCategory value : values()) {
            if (value.structTypes.contains(structType)) {
                constraints.add(value);
            }
        }
        return constraints;
    }

}
