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

package com.antgroup.geaflow.dsl.common.util;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.IType;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;

public class TypeCastUtil {

    public static Object cast(Object o, IType<?> type) {
        return cast(o, type.getTypeClass());
    }

    public static Object cast(Object o, Class<?> targetType) {
        if (o == null) {
            return null;
        }
        if (targetType.isAssignableFrom(o.getClass())) {
            return o;
        }
        if (targetType == Object.class) {
            return o;
        }
        if (targetType.isArray()) {
            if (o.getClass().isArray()) {
                int length = Array.getLength(o);
                Object castArray = Array.newInstance(targetType.getComponentType(), length);
                for (int i = 0; i < length; i++) {
                    Object element = Array.get(o, i);
                    Array.set(castArray, i, cast(element, targetType.getComponentType()));
                }
                return castArray;
            }
        }
        if (o instanceof Integer) {
            if (targetType == Long.class) {
                return ((Integer) o).longValue();
            }
            if (targetType == Double.class) {
                return ((Integer) o).doubleValue();
            }
            if (targetType == BigDecimal.class) {
                return new BigDecimal((Integer) o);
            }
            if (targetType == String.class) {
                return String.valueOf(o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
        }
        if (o.getClass() == Long.class) {
            if (targetType == Double.class) {
                return ((Long) o).doubleValue();
            }
            if (targetType == BigDecimal.class) {
                return new BigDecimal((Long) o);
            }
            if (targetType == Integer.class) {
                return ((Long) o).intValue();
            }
            if (targetType == Short.class) {
                return ((Long) o).shortValue();
            }
            if (targetType == String.class) {
                return String.valueOf(o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
            if (targetType == Timestamp.class) {
                return new Timestamp((long) o);
            }
        }
        if (o.getClass() == Byte.class) {
            if (targetType == Integer.class) {
                return ((Byte) o).intValue();
            }
            if (targetType == Long.class) {
                return ((Byte) o).longValue();
            }
            if (targetType == Short.class) {
                return ((Byte) o).shortValue();
            }
            if (targetType == Double.class) {
                return ((Byte) o).doubleValue();
            }
            if (targetType == BigDecimal.class) {
                return new BigDecimal((Byte) o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
        }
        if (o.getClass() == Short.class) {
            if (targetType == Integer.class) {
                return ((Short) o).intValue();
            }
            if (targetType == Long.class) {
                return ((Short) o).longValue();
            }
            if (targetType == Double.class) {
                return ((Short) o).doubleValue();
            }
            if (targetType == BigDecimal.class) {
                return new BigDecimal(((Short) o));
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
        }
        if (o.getClass() == BigDecimal.class) {
            if (targetType == Double.class) {
                return ((BigDecimal) o).doubleValue();
            }
        }
        if (o.getClass() == String.class) {
            if (targetType == Long.class) {
                return Long.parseLong((String) o);
            }
            if (targetType == Integer.class) {
                return Integer.parseInt((String) o);
            }
            if (targetType == Double.class) {
                return Double.valueOf((String) o);
            }
            if (targetType == Boolean.class) {
                return Boolean.valueOf((String) o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(o.toString());
            }
            if (targetType == Timestamp.class) {
                return Timestamp.valueOf((String) o);
            }
        }
        if (o.getClass() == BinaryString.class) {
            if (targetType == Long.class) {
                return Long.parseLong(o.toString());
            }
            if (targetType == Integer.class) {
                return Integer.parseInt(o.toString());
            }
            if (targetType == Double.class) {
                return Double.valueOf(o.toString());
            }
            if (targetType == Boolean.class) {
                return Boolean.valueOf(o.toString());
            }
            if (targetType == String.class) {
                return o.toString();
            }
            if (targetType == Timestamp.class) {
                return Timestamp.valueOf(o.toString());
            }
        }
        if (o.getClass() == Double.class) {
            if (targetType == BigDecimal.class) {
                return new BigDecimal((Double) o);
            }
            if (targetType == Long.class) {
                return ((Double) o).longValue();
            }
            if (targetType == Integer.class) {
                return ((Double) o).intValue();
            }
            if (targetType == Short.class) {
                return ((Double) o).shortValue();
            }
            if (targetType == String.class) {
                return String.valueOf(o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
        }
        if (o.getClass() == Boolean.class) {
            if (targetType == String.class) {
                return String.valueOf(o);
            }
            if (targetType == BinaryString.class) {
                return BinaryString.fromString(String.valueOf(o));
            }
        }
        throw new IllegalArgumentException("Cannot cast " + o + " from " + o.getClass() + " to " + targetType);
    }
}
