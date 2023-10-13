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

package com.antgroup.geaflow.dsl.schema.function;

import com.antgroup.geaflow.dsl.calcite.MetaFieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlSumAggFunction;

public class GqlSumAggFunction extends SqlSumAggFunction {

    public GqlSumAggFunction(RelDataType type) {
        super(type instanceof MetaFieldType ? ((MetaFieldType) type).getType() : type);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType superType = super.inferReturnType(opBinding);
        if (superType instanceof MetaFieldType) {
            return ((MetaFieldType) superType).getType();
        }
        return superType;
    }
}
