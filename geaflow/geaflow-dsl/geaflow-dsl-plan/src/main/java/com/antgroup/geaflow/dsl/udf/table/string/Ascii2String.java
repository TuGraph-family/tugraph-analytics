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

@Description(name = "ascii2str", description = "Convert ascii code to string.")
public class Ascii2String extends UDF {

    public String eval(Integer ascii) {
        if (ascii == null) {
            return null;
        }
        return new String(new byte[]{ascii.byteValue()});
    }

    public String eval(Long ascii) {
        if (ascii == null) {
            return null;
        }
        return eval(ascii.intValue());
    }
}
