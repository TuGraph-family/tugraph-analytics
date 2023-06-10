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

package com.antgroup.geaflow.dsl.udf.date;

import com.antgroup.geaflow.dsl.udf.table.date.LastDay;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LastDayTest {

    LastDay lastDay;

    @BeforeClass
    public void before() {
        lastDay = new LastDay();
    }

    @Test
    public void testEval() {
        String day = lastDay.eval("2018-04-23 12:00:01");
        Assert.assertEquals(day,"2018-04-30 00:00:00");
    }
}
