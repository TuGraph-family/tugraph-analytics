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

@Description(name = "rtrim", description = "Returns a string with the right space removed.")
public class RTrim extends UDF {

    public String eval(String s) {
        if (s == null) {
            return null;
        }
        return StringUtils.stripEnd(s, " ");
    }

    public BinaryString eval(BinaryString s) {
        if (s == null) {
            return null;
        }
        int r = s.getLength() - 1;
        while (r >= 0) {
            if (s.getByte(r) == ' ') {
                r--;
            } else {
                break;
            }
        }
        return s.substring(0, r + 1);
    }
}
