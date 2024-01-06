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

import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import org.testng.annotations.Test;

public class DistinctTest {

    @Test
    public void testDistinct_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/distinct_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/distinct_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/distinct_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/distinct_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/distinct_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_006() throws Exception {
        QueryTester
            .build()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "4")
            .withQueryPath("/query/distinct_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testDistinct_007() throws Exception {
        QueryTester
            .build()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), "4")
            .withQueryPath("/query/distinct_007.sql")
            .execute()
            .checkSinkResult();
    }
}
