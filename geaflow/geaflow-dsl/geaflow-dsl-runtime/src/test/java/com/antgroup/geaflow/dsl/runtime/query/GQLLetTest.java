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

public class GQLLetTest {

    @Test
    public void testLet_001() throws Exception {
       QueryTester
           .build()
           .withGraphDefine("/query/modern_graph.sql")
           .withQueryPath("/query/gql_let_001.sql")
           .execute()
           .checkSinkResult();
    }

    @Test
    public void testLet_002() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_let_002.sql")
            .execute()
            .checkSinkResult();
    }

    @Test
    public void testLet_003() throws Exception {
        QueryTester
            .build()
            .withGraphDefine("/query/modern_graph.sql")
            .withQueryPath("/query/gql_let_003.sql")
            .execute()
            .checkSinkResult();
    }
}
