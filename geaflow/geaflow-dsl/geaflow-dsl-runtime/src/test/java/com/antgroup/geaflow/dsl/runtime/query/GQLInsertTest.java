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

import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.annotations.Test;

public class GQLInsertTest {

    @Test
    public void testInsertAndQuery_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_graph_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQuery_003() throws Exception {
        QueryTester
            .build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), "2")
            .withQueryPath("/query/gql_insert_and_graph_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithRequest_003() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_request_003.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithSubQuery_001() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_subquery_001.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testInsertAndQueryWithSubQuery_002() throws Exception {
        QueryTester
            .build()
            .withQueryPath("/query/gql_insert_and_query_with_subquery_002.sql")
            .execute()
            .checkSinkResult();
    }
}
