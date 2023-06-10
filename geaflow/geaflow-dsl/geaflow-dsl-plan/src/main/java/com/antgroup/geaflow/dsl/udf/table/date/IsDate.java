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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "isdate", description = "Returns whether the string is a date format.")
public class IsDate extends UDF {
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private DateTimeFormatter lastFormatter;
    private String lastFormat;

    public Boolean eval(String date, String format) {
        if (date == null || format == null) {
            return false;
        }
        if (lastFormat == null || !lastFormat.equals(format)) {
            lastFormatter = DateTimeFormat.forPattern(format);
            lastFormat = format;
        }
        try {
            lastFormatter.parseDateTime(date);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Boolean eval(String date) {
        return eval(date, DEFAULT_FORMAT);
    }
}
