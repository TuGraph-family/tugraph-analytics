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

package com.antgroup.geaflow.dsl.common.compile;

import java.io.Serializable;
import java.util.Objects;

public class TableInfo implements Serializable {

    private final String instanceName;

    private final String tableName;

    public TableInfo(String instanceName, String tableName) {
        this.instanceName = instanceName;
        this.tableName = tableName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableInfo)) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        return Objects.equals(tableName, that.tableName) &&  Objects.equals(instanceName, that.instanceName) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, instanceName);
    }
}
