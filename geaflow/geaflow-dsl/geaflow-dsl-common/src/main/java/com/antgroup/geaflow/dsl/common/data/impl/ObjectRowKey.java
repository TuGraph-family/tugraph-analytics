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

package com.antgroup.geaflow.dsl.common.data.impl;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.RowKey;
import java.util.Arrays;
import java.util.Objects;

public class ObjectRowKey implements RowKey {

    private final Object[] keys;

    private ObjectRowKey(Object... keys) {
        this.keys = Objects.requireNonNull(keys);
    }

    public static RowKey of(Object... keys) {
        return new ObjectRowKey(keys);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return keys[i];
    }

    @Override
    public Object[] getKeys() {
        return keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectRowKey)) {
            return false;
        }
        ObjectRowKey that = (ObjectRowKey) o;
        return Arrays.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keys);
    }

    @Override
    public String toString() {
        return "ObjectRowKey{"
            + "keys=" + Arrays.toString(keys)
            + '}';
    }
}
