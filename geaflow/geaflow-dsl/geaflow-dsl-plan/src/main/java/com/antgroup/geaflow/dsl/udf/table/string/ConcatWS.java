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
import org.apache.commons.lang3.StringUtils;

@Description(name = "concat_ws", description = "Concat strings to one by the separator string.")
public class ConcatWS extends UDF {

    public String eval(String separator, String... args) {
        return StringUtils.join(args, separator);
    }

    public BinaryString eval(String separator, BinaryString... args) {
        BinaryString sep = Objects.isNull(separator) ? BinaryString.EMPTY_STRING :
                           BinaryString.fromString(separator);
        return BinaryString.concatWs(sep, args);
    }

    public BinaryString eval(BinaryString separator, BinaryString... args) {
        return BinaryString.concatWs(separator, args);
    }
}
