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

package com.antgroup.geaflow.dsl.udf.table.math;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import java.lang.Math;

@Description(name = "log2", description = "Returns the logarithm base 2.")
public class Log2 extends UDF {

    private static final double LOG2 = Math.log(2.0);

    public Double eval(Double a) {
        if (a == null || a <= 0.0) {
            return null;
        } else {
            return Math.log(a) / LOG2;
        }
    }

}
