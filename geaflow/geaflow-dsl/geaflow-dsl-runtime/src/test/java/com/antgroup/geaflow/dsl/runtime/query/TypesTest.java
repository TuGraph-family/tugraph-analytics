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

package com.antgroup.geaflow.dsl.runtime.query;

import org.testng.annotations.Test;

public class TypesTest {

    @Test
    public void testBooleanType_001() throws Exception  {
        QueryTester
            .build()
            .withQueryPath("/query/type_boolean_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testTimestampType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_timestamp_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDateType_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/type_date_001.sql")
            .execute()
            .checkSinkResult();
    }
}
