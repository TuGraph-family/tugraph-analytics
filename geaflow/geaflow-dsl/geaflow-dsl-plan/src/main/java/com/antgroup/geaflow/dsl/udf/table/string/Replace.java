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

@Description(name = "replace", description = "Removes each substring of the source String that matches"
    + " the regular expression.")
public class Replace extends UDF {

    public String eval(String text, String searchString, String replacement) {
        return StringUtils.replace(text, searchString, replacement);
    }

    public String eval(Object text, String searchString, String replacement) {
        return StringUtils.replace(String.valueOf(text), searchString, replacement);
    }

}
