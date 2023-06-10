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

import org.testng.Assert;
import org.testng.annotations.Test;

public class DateTimeUtilTest {

    @Test
    public void testDateTimeUtil() {
        Assert.assertEquals(DateTimeUtil.fromUnixTime(1111, "yyyy-MM-dd hh:mm:ss"),
            "1970-01-01 08:18:31");
        Assert.assertEquals(DateTimeUtil.toUnixTime("1970-01-01 08:18:31", "yyyy-MM-dd hh:mm:ss"),
            1111);
        Assert.assertEquals(DateTimeUtil.toUnixTime("", "yyyy-MM-dd hh:mm:ss"),
            -1);
    }
}
