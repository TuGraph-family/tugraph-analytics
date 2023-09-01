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

package com.antgroup.geaflow.dsl.planner;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.parser.GeaFlowDSLParser;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.validator.GQLValidatorImpl;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.annotations.Test;

public class GQLContextTest {
    @Test
    public void testGQLContext() throws SqlParseException {
        String stmt = "Create Graph g (\n" + "  Vertex buyer \n"
            + "  (id bigint ID, name string, age int),\n" + "  Edge knows \n"
            + "  (s_id bigint SOURCE ID, t_id bigint DESTINATION ID, weight double)\n"+ ")"
            + " with (store = 'memory')\n";

        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        SqlNode sqlNode = parser.parseStatement(stmt);
        assertTrue(sqlNode instanceof SqlCreateGraph);
        SqlCreateGraph sqlCreateGraph = (SqlCreateGraph) sqlNode;
        GQLContext gqlContext = GQLContext.create(new Configuration(), false);
        GeaFlowGraph graph = gqlContext.convertToGraph(sqlCreateGraph);
        gqlContext.registerGraph(graph);
        assertNull(gqlContext.findSqlFunction(null,"function"));
        assertNotNull(gqlContext.getTypeFactory());
        assertTrue(gqlContext.getRelBuilder() instanceof GQLRelBuilder);
        assertNotNull(gqlContext.getValidator());
        gqlContext.setCurrentGraph("g");
        assertEquals(gqlContext.getCurrentGraph(), "g");
    }
}
