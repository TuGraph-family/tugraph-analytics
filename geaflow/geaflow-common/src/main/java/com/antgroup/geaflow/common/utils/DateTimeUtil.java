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

package com.antgroup.geaflow.common.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeUtil {

    public static int toUnixTime(String dateStr, String format) {
        if (dateStr == null || dateStr.isEmpty()) {
            return -1;
        } else {
            DateTimeFormatter dateTimeFormat = DateTimeFormat.forPattern(format);
            return (int) (dateTimeFormat.parseMillis(dateStr) / 1000);
        }
    }

    public static String fromUnixTime(int unixTime, String format) {
        long millsTs = ((long) unixTime) * 1000L;
        return DateTimeFormat.forPattern(format).print(millsTs);
    }

    public static String fromUnixTime(long unixTime, String format) {
        return DateTimeFormat.forPattern(format).print(unixTime);
    }
}
