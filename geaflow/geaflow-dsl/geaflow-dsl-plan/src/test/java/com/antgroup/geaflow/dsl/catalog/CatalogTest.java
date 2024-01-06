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

package com.antgroup.geaflow.dsl.catalog;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.catalog.exception.ObjectAlreadyExistException;
import com.antgroup.geaflow.dsl.catalog.exception.ObjectNotExistException;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CatalogTest {

    @Test
    public void testMemoryCatalog() {
        Catalog catalog = CatalogFactory.getCatalog(new Configuration());
        String instance = "default";
        GeaFlowGraph graph = new GeaFlowGraph(instance, "g1", new ArrayList<>(),
            new ArrayList<>(), new HashMap<>(), new HashMap<>(), true, false);
        GeaFlowTable table = new GeaFlowTable(instance, "t1", new ArrayList<>(),
            new ArrayList<>(), new ArrayList<>(), new HashMap<>(), true, false);
        GeaFlowView view = new GeaFlowView(instance, "v1", new ArrayList<>(),
            null, null, true);
        catalog.createGraph(graph.getInstanceName(), graph);
        catalog.createTable(table.getInstanceName(), table);
        catalog.createView(view.getInstanceName(), view);
        // create repeatedly
        catalog.createGraph(instance, graph);
        catalog.createTable(instance, table);
        catalog.createView(instance, view);

        Set<String> graphAndTables = catalog.listGraphAndTable(instance);
        Assert.assertEquals(graphAndTables.size(), 3);
        catalog.describeGraph(instance, "g1");
        catalog.describeTable(instance, "t1");
        catalog.dropGraph(instance, "g1");
        catalog.dropTable(instance, "t1");
        Set<String> graphAndTablesAfterDrop = catalog.listGraphAndTable(instance);
        Assert.assertEquals(graphAndTablesAfterDrop.size(), 1);

        // check exception
        try {
            catalog.dropGraph("testInstance", "g1");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ObjectNotExistException);
        }
        try {
            catalog.dropGraph(instance, "g1");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ObjectNotExistException);
        }
        try {
            GeaFlowGraph graph2 = new GeaFlowGraph(instance, "g2", new ArrayList<>(),
                new ArrayList<>(), new HashMap<>(), new HashMap<>(), false, false);
            catalog.createGraph(instance, graph2);
            catalog.createGraph(instance, graph2);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ObjectAlreadyExistException);
        }
    }
}
