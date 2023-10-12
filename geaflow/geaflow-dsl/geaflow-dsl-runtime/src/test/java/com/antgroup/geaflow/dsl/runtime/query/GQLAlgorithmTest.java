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

public class GQLAlgorithmTest {

    @Test
    public void testAlgorithm_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_algorithm_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_004() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_005() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testAlgorithm_006() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_006.sql")
            .execute()
            .checkSinkResult();
    }
    @Test
    public void testAlgorithm_007() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_algorithm_007.sql")
            .execute()
            .checkSinkResult();
    }
}
