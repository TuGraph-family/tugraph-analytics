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

package com.antgroup.geaflow.dsl.udf.table.other;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;

@Description(name = "is_decimal", description = "Returns true if only contains digits and is "
    + "non-null, otherwise return false.")
public class IsDecimal extends UDF {

    public boolean eval(String s) {
        if (s == null) {
            return false;
        }
        if (isInteger(s) || isLong(s) || isDouble(s)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean isInteger(String s) {
        boolean flag = true;
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            flag = false;
        }
        return flag;
    }

    private boolean isLong(String s) {
        boolean flag = true;
        try {
            Long.parseLong(s);
        } catch (NumberFormatException e) {
            flag = false;
        }
        return flag;
    }

    private boolean isDouble(String s) {
        boolean flag = true;
        try {
            Double.parseDouble(s);
        } catch (NumberFormatException e) {
            flag = false;
        }
        return flag;
    }
}
