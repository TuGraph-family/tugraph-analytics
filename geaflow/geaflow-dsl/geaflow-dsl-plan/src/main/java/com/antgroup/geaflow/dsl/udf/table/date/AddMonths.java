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
import java.util.Calendar;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@Description(name = "add_months", description = "Returns the date that is num_months after "
    + "start_date.")
public class AddMonths extends UDF {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(
        "yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");
    private final Calendar calendar = Calendar.getInstance();


    public String eval(String date, Integer month) {
        if (date == null || month == null) {
            return null;
        }
        DateTimeFormatter formatter;

        if (date.length() <= 10) {
            formatter = DATE_FORMATTER;
        } else {
            formatter = DATE_TIME_FORMATTER;
        }
        try {
            calendar.setTime(formatter.parseDateTime(date).toDate());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
        calendar.add(Calendar.MONTH, month);
        try {
            return formatter.print(calendar.getTime().getTime());
        } catch (Exception e) {
            throw new GeaflowRuntimeException(e);
        }
    }
}
