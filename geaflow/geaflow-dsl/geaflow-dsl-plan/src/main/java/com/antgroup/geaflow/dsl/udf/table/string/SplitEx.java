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

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import org.apache.commons.lang3.StringUtils;

@Description(name = "split_ex", description = "Split string by separator and returns nth substring")
public class SplitEx extends UDF {

    public String eval(String str, String separator, Integer index) {
        if ((str == null) || separator == null || index == null) {
            return null;
        }
        if (index < 0) {
            return null;
        }
        String[] values = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, separator);
        if (index >= values.length) {
            return null;
        }
        return values[index];
    }

    public BinaryString eval(BinaryString str, BinaryString separator, Integer index) {
        if ((str == null) || separator == null || separator.equals(BinaryString.EMPTY_STRING) || index == null) {
            return null;
        }
        if (index < 0) {
            return null;
        }
        String[] values = StringUtils.splitByWholeSeparatorPreserveAllTokens(str.toString(), separator.toString());
        if (index >= values.length) {
            return null;
        }
        return BinaryString.fromString(values[index]);
    }
}
