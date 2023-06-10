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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Description(name = "regexp", description = "Returns whether the string match the regular expression.")
public class RegExp extends UDF {

    private String lastRegex;
    private Pattern p = null;

    public Boolean eval(String s, String regex) {
        if (s == null || regex == null) {
            return null;
        }
        if (regex.length() == 0) {
            return false;
        }
        if (!regex.equals(lastRegex) || p == null) {
            lastRegex = regex;
            p = Pattern.compile(regex);
        }
        Matcher m = p.matcher(s);
        return m.find(0);
    }
}
