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

@Description(name = "from_unixtime_millis", description = "Translate unix timestamp to date string.")
public class FromUnixTimeMillis extends UDF {

    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    private DateTimeFormatter lastFormatter;
    private String lastFormat;

    public String eval(String unixTime) {
        return eval(Long.valueOf(unixTime), DEFAULT_FORMAT);
    }

    public String eval(Long unixTime, String format) {
        if (unixTime == null || format == null) {
            return null;
        }
        return evaluate(unixTime, format);
    }

    public String eval(Long unixTime) {
        if (unixTime == null) {
            return null;
        }
        return eval(unixTime, DEFAULT_FORMAT);
    }

    private String evaluate(Long unixTime, String format) {
        if (!format.equals(lastFormat)) {
            lastFormatter = DateTimeFormat.forPattern(format);
            lastFormat = format;
        }

        return lastFormatter.print(unixTime);
    }
}
