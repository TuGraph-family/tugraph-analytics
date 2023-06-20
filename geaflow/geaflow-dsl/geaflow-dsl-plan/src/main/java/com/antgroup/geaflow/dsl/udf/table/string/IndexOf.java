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

@Description(name = "index_of", description = "Returns the position of the first occurrence of the target string in the string.")
public class IndexOf extends UDF {

    public Integer eval(String str, String target) {
        return eval(str, target, 0);
    }

    public Integer eval(String str, String target, Integer index) {
        if ((str == null) || (target == null) || (index == null)) {
            return -1;
        }
        int fromIndex = index;
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        return str.indexOf(target, fromIndex);
    }

    public Integer eval(BinaryString str, BinaryString target) {
        return eval(str, target, 0);
    }

    public Integer eval(BinaryString str, BinaryString target, Integer index) {
        if ((str == null) || (target == null) || (index == null)) {
            return -1;
        }
        int fromIndex = index;
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        return str.indexOf(target, fromIndex);
    }
}
