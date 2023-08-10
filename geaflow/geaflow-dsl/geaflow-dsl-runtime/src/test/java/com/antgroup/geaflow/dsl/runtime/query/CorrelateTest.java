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

public class CorrelateTest {

    @Test
    public void testCorrelate_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/correlate_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testCorrelate_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/correlate_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testCorrelate_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/correlate_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testCorrelate_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/correlate_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testCorrelate_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/correlate_005.sql")
            .execute()
            .checkSinkResult();
    }
}
