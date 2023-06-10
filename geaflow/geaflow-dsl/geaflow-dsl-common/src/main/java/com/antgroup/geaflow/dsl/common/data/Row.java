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

package com.antgroup.geaflow.dsl.common.data;

import com.antgroup.geaflow.common.type.IType;

public interface Row extends StepRecord {

    Object getField(int i, IType<?> type);

    Row EMPTY = (i, type) -> {
        throw new IllegalArgumentException("Cannot getField from empty row");
    };

    default StepRecordType getType() {
        return StepRecordType.ROW;
    }

    default Object[] getFields(IType<?>[] types) {
        Object[] fields = new Object[types.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = getField(i, types[i]);
        }
        return fields;
    }
}
