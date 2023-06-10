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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

@Description(name = "regexp_extract", description = "Extract the substring match the regular expression.")
public class RegExpExtract extends UDF {

    private String lastRegex = null;
    private Pattern p = null;

    public String eval(String s, String regex, String extractIndex) {
        return eval(s, regex, Long.valueOf(extractIndex));
    }

    public String eval(String s, String regex, Integer extractIndex) {
        if (s == null || regex == null || extractIndex == null) {
            return null;
        }
        if (StringUtils.isEmpty(regex)) {
            return null;
        }
        if (extractIndex < 0) {
            return null;
        }
        if (!regex.equals(lastRegex) || p == null) {
            lastRegex = regex;
            p = Pattern.compile(regex);
        }
        Matcher m = p.matcher(s);
        if (m.find()) {
            MatchResult mr = m.toMatchResult();
            return mr.group(extractIndex);
        }
        return "";
    }

    public String eval(Object s, String regex, Long extractIndex) {
        return eval(String.valueOf(s), regex, extractIndex);
    }

    public String eval(String s, String regex, Long extractIndex) {
        if (s == null || regex == null || extractIndex == null) {
            return null;
        }

        if (StringUtils.isEmpty(regex)) {
            return null;
        }
        if (extractIndex < 0) {
            return null;
        }
        return eval(s, regex, extractIndex.intValue());
    }

    public String eval(String s, String regex) {
        return this.eval(s, regex, 1);
    }

    public String eval(Object s, String regex) {
        return this.eval(String.valueOf(s), regex, 1);
    }
}
