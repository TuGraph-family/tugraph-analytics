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

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.function.UDF;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "day_of_month", description =
    "Returns the date of the month of date.")
public class DayOfMonth extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private final Calendar calendar = Calendar.getInstance();

    public Integer eval(String dateString) {
        if (dateString == null) {
            return null;
        }
        DateTimeFormatter formatter;
        try {
            if (dateString.length() <= 10) {
                formatter = DATE_FORMATTER;
            } else {
                formatter = DATE_TIME_FORMATTER;
            }
            Date date = formatter.parseDateTime(dateString).toDate();
            calendar.setTime(date);
            return calendar.get(Calendar.DAY_OF_MONTH);
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public Integer eval(Timestamp t) {
        if (t == null) {
            return null;
        }
        calendar.setTime(t);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }
}
