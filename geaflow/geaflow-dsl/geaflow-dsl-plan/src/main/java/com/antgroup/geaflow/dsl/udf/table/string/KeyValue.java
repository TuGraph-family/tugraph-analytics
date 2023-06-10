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

package com.antgroup.geaflow.dsl.udf.table.string;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import org.apache.commons.lang3.StringUtils;

@Description(name = "keyvalue", description = "Split the string to get key-value and return the value "
    + "for specified key.")
public class KeyValue extends UDF {

    public String eval(Object value, String lineDelimiter, String colDelimiter, String key) {
        return eval(String.valueOf(value), lineDelimiter, colDelimiter, key);
    }

    public String eval(String value, String lineDelimiter, String colDelimiter, String key) {
        if (value == null) {
            return null;
        }

        String[] lines = StringUtils.splitByWholeSeparator(value, lineDelimiter);
        for (String line : lines) {
            if (StringUtils.isBlank(line)) {
                continue;
            }
            String[] keyValue = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, colDelimiter);
            if (key.equals(keyValue[0])) {
                if (keyValue.length == 2) {
                    return keyValue[1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }
}
