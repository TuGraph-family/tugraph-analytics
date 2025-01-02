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

package com.antgroup.geaflow.dsl.connector.odps.utils;

import com.aliyun.odps.OdpsType;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.primitive.BinaryStringType;
import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.ByteType;
import com.antgroup.geaflow.common.type.primitive.DecimalType;
import com.antgroup.geaflow.common.type.primitive.DoubleType;
import com.antgroup.geaflow.common.type.primitive.FloatType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.common.type.primitive.ShortType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.common.type.primitive.TimestampType;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.dsl.common.types.VoidType;

public class OdpsConnectorUtils {

    public static boolean typeEquals(OdpsType odpsType, IType itype) {
        switch (odpsType) {
            case TINYINT:
            case SMALLINT:
                return itype instanceof LongType || itype instanceof IntegerType
                    || itype instanceof ShortType;
            case INT:
                return itype instanceof LongType || itype instanceof IntegerType;
            case BIGINT:
                return itype instanceof IntegerType;
            case FLOAT:
                return itype instanceof FloatType || itype instanceof DoubleType;
            case DOUBLE:
                return itype instanceof DoubleType;
            case BOOLEAN:
                return itype instanceof BooleanType;
            case CHAR:
            case STRING:
            case VARCHAR:
                return itype instanceof BinaryStringType || itype instanceof StringType;
            case BINARY:
                return itype instanceof ByteType || itype instanceof BinaryStringType;
            case DECIMAL:
                return itype instanceof DecimalType;
            case ARRAY:
                return itype instanceof ArrayType;
            case VOID:
                return itype instanceof VoidType;
            case DATETIME:
            case DATE:
                return itype instanceof TimestampType;
            case TIMESTAMP:
                return itype instanceof TimestampType || itype instanceof LongType;
            case MAP:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
            case STRUCT:
            default:
                return false;
        }
    }
}
