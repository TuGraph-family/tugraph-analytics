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

public class GQLSortTest {

    @Test
    public void testSort_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_006.sql")
            .compareWithOrder()
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_007.sql")
            .compareWithOrder()
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_008() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_008.sql")
            .compareWithOrder()
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_009() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_009.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testSort_010() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_sort_010.sql")
            .compareWithOrder()
            .execute()
            .checkSinkResult();
    }
}
