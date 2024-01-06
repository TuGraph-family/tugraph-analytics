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
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import org.testng.annotations.Test;

public class GQLWriteAndReadTest {

    @Test
    public void testInsertDynamicGraph_001() throws Exception {
        QueryTester
            .build()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), 1)
            .withQueryPath("/query/gql_graph_write_001.sql")
            .execute() // write data to graph
            .withQueryPath("/query/gql_graph_read_001.sql")
            .execute() // query the graph
            .checkSinkResult();
    }

    @Test
    public void testInsertDynamicGraph_002() throws Exception {
        QueryTester
            .build()
            .withConfig(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), -1)
            .withQueryPath("/query/gql_graph_write_002.sql")
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), 1)
            .execute() // write data to graph
            .withQueryPath("/query/gql_graph_write_003.sql")
            .execute() // write data to graph
            .withQueryPath("/query/gql_graph_read_002.sql")
            .execute() // query the graph
            .checkSinkResult();
    }

    @Test
    public void testInsertStaticGraph_001() throws Exception {
        QueryTester
            .build()
            .withConfig(FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT.getKey(), 1)
            .withQueryPath("/query/gql_static_graph_001.sql")
            .execute() // write data to graph
            .withQueryPath("/query/gql_static_graph_read_001.sql")
            .execute() // query the graph
            .checkSinkResult();
    }
}
