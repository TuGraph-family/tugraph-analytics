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

package com.antgroup.geaflow.dsl.common.types;

import com.antgroup.geaflow.common.type.IType;
import java.io.Serializable;
import java.util.Objects;

public class TableField implements Serializable {

    private final String name;

    private final IType<?> type;

    private final boolean nullable;

    public TableField(String name, IType<?> type, boolean nullable) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.nullable = nullable;
    }

    public TableField(String name, IType<?> type) {
        this(name, type, true);
    }

    public String getName() {
        return name;
    }

    public IType<?> getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public TableField copy(String name) {
        return new TableField(name, type, nullable);
    }

    public TableField copy(IType<?> type) {
        return new TableField(name, type, nullable);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableField)) {
            return false;
        }
        TableField field = (TableField) o;
        return nullable == field.nullable && Objects.equals(name, field.name) && Objects.equals(type,
            field.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, nullable);
    }

    @Override
    public String toString() {
        return "TableField{"
            + "name='" + name + '\''
            + ", type=" + type
            + ", nullable=" + nullable
            + '}';
    }
}
