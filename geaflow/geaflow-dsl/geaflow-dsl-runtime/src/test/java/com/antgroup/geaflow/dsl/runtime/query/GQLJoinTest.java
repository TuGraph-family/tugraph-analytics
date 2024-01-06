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

public class GQLJoinTest {

    @Test
    public void testJoin_001() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_004() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_004.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_005() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_005.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_006() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_006.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testJoin_007() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_join_007.sql")
            .execute()
            .checkSinkResult();
    }
}
