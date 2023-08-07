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

package com.antgroup.geaflow.dsl.calcite;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class JoinPathRecordType extends PathRecordType {

    public JoinPathRecordType(List<RelDataTypeField> fields) {
        super(fields);
    }

    @Override
    public PathRecordType copy(int index, RelDataType newType) {
        PathRecordType recordType = super.copy(index, newType);
        return new JoinPathRecordType(recordType.getFieldList());
    }

    @Override
    public boolean isSinglePath() {
        return false;
    }
}