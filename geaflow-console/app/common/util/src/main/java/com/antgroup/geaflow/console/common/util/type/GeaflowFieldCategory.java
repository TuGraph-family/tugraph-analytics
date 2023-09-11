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

import static com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory.NumConstraint.AT_MOST_ONCE;
import static com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory.NumConstraint.EXACTLY_ONCE;
import static com.antgroup.geaflow.console.common.util.type.GeaflowFieldCategory.NumConstraint.NONE;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public enum GeaflowFieldCategory {

    PROPERTY(NONE, GeaflowStructType.values()),

    ID(EXACTLY_ONCE, GeaflowStructType.TABLE, GeaflowStructType.VIEW),

    VERTEX_ID(EXACTLY_ONCE, GeaflowStructType.VERTEX),

    VERTEX_LABEL(EXACTLY_ONCE, GeaflowStructType.VERTEX),

    EDGE_SOURCE_ID(EXACTLY_ONCE, GeaflowStructType.EDGE),

    EDGE_TARGET_ID(EXACTLY_ONCE, GeaflowStructType.EDGE),

    EDGE_LABEL(EXACTLY_ONCE, GeaflowStructType.EDGE),

    EDGE_TIMESTAMP(AT_MOST_ONCE, GeaflowStructType.EDGE);

    private final Set<GeaflowStructType> structTypes;

    private final NumConstraint numConstraint;

    GeaflowFieldCategory(NumConstraint numConstraint, GeaflowStructType... structTypes) {
        this.numConstraint = numConstraint;
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

    enum NumConstraint {
        /**
         * count == 1.
         */
        EXACTLY_ONCE,
        /**
         * count <= 1.
         */
        AT_MOST_ONCE,
        NONE
    }

    public void validate(int count) {
        switch (this.numConstraint) {
            case EXACTLY_ONCE:
                if (count < 1) {
                    throw new GeaflowException("Must have {} field", this.name());
                } else if (count > 1) {
                    throw new GeaflowException("Can have only one {} field", this.name());
                }
                break;
            case AT_MOST_ONCE:
                if (count > 1) {
                    throw new GeaflowException("Can have only one {} field", this.name());
                }
                break;
            default:
                return;
        }
    }
}
