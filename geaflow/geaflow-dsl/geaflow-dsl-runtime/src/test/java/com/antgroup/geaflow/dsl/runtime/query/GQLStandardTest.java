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

public class GQLStandardTest {

    @Test
    public void testStandard_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_001.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_002.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_003.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_004.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_005.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_006.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_007.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_008.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_009.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_010.sql")
            .execute()
            .checkSinkResult("standard");
    }

    @Test
    public void testStandard_011() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/movie_graph.sql")
            .withQueryPath("/query/standard/standard_gql_011.sql")
            .execute()
            .checkSinkResult("standard");
    }
}
