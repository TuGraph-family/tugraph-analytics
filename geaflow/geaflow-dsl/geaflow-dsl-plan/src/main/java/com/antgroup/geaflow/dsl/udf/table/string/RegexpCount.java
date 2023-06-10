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

@Description(name = "regexp_count", description = "Returns the count that the pattern matched in the string.")
public class RegexpCount extends UDF {

    private String lastPattern;
    private Pattern p = null;

    public Long eval(String source, String pattern, Long startPos) {
        if (source == null || pattern == null || startPos == null) {
            return null;
        }
        if (lastPattern == null || !lastPattern.equals(pattern)) {
            p = Pattern.compile(pattern);
            lastPattern = pattern;
        }
        Matcher matcher = p.matcher(source.substring(startPos.intValue()));
        long c = 0;
        while (matcher.find()) {
            c++;
        }
        return c;
    }

    public Long eval(String source, String pattern) {
        return eval(source, pattern, 0L);
    }
}
