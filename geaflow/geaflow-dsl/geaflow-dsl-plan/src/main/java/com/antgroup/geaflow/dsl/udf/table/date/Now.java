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

package com.antgroup.geaflow.dsl.udf.table.date;

import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import java.util.Date;

@Description(name = "now", description = "Returns current timestamp.")
public class Now extends UDF {

    public Long eval() {
        Date date = new Date();
        return date.getTime() / 1000;
    }

    public Long eval(Integer offset) {
        Date date = new Date();
        return date.getTime() / 1000 + offset;
    }

    public Long eval(Long offset) {
        Date date = new Date();
        return date.getTime() / 1000 + offset;
    }
}
