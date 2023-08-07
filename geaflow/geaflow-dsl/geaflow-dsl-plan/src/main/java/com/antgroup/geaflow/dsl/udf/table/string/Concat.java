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
import java.util.Objects;

@Description(name = "concat", description = "Concat strings to one string.")
public class Concat extends UDF {

    public String eval(String... args) {
        if (Objects.isNull(args)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            if (arg != null) {
                sb.append(arg);
            }
        }
        return sb.toString();
    }

    public BinaryString eval(BinaryString... args) {
        if (Objects.isNull(args)) {
            return null;
        }
        return BinaryString.concat(args);
    }
}
