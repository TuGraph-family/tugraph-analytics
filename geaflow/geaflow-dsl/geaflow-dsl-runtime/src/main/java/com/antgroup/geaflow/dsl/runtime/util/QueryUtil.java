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

package com.antgroup.geaflow.dsl.runtime.util;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.parser.GeaFlowDSLParser;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlEdgeUsing;
import com.antgroup.geaflow.dsl.sqlnode.SqlVertexUsing;
import com.antgroup.geaflow.dsl.util.GQLNodeUtil;
import com.antgroup.geaflow.dsl.util.StringLiteralUtil;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;

public class QueryUtil {

    public static PreCompileResult preCompile(String script, Configuration config) {
        GeaFlowDSLParser parser = new GeaFlowDSLParser();
        GQLContext gqlContext = GQLContext.create(config, false);
        PreCompileResult preCompileResult = new PreCompileResult();
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(script);
            List<GeaFlowTable> createTablesInScript = new ArrayList<>();
            Map<String, SqlCreateGraph> createGraphs = new HashMap<>();
            for (SqlNode sqlNode : sqlNodes) {
                if (sqlNode instanceof SqlSetOption) {
                    SqlSetOption sqlSetOption = (SqlSetOption) sqlNode;
                    String key = StringLiteralUtil.toJavaString(sqlSetOption.getName());
                    String value = StringLiteralUtil.toJavaString(sqlSetOption.getValue());
                    config.put(key, value);
                } else if (sqlNode instanceof SqlCreateTable) {
                    createTablesInScript.add(gqlContext.convertToTable((SqlCreateTable) sqlNode));
                } else if (sqlNode instanceof SqlCreateGraph) {
                    SqlCreateGraph createGraph = (SqlCreateGraph) sqlNode;
                    SqlIdentifier graphName = gqlContext.completeCatalogObjName(createGraph.getName());
                    if (createGraph.getVertices().getList().stream().anyMatch(node -> node instanceof SqlVertexUsing)
                        || createGraph.getEdges().getList().stream().anyMatch(node -> node instanceof SqlEdgeUsing)) {
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph,
                            createTablesInScript, config);
                        preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, config));
                    } else {
                        createGraphs.put(graphName.toString(), createGraph);
                    }
                } else if (sqlNode instanceof SqlInsert) {
                    SqlInsert insert = (SqlInsert) sqlNode;
                    SqlIdentifier insertName = gqlContext.completeCatalogObjName(
                        (SqlIdentifier) insert.getTargetTable());
                    SqlIdentifier insertGraphName = GQLNodeUtil.getGraphTableName(insertName);
                    if (createGraphs.containsKey(insertGraphName.toString())) {
                        SqlCreateGraph createGraph = createGraphs.get(insertGraphName.toString());
                        GeaFlowGraph graph = gqlContext.convertToGraph(createGraph, config);
                        preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(graph, config));
                    } else {
                        Table graph = gqlContext.getCatalog().getGraph(gqlContext.getCurrentInstance(),
                            insertGraphName.toString());
                        if (graph != null) {
                            GeaFlowGraph geaFlowGraph = (GeaFlowGraph) graph;
                            preCompileResult.addGraph(SchemaUtil.buildGraphViewDesc(geaFlowGraph, config));
                        }
                    }
                }
            }
            return preCompileResult;
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException(e);
        }
    }


    public static class PreCompileResult implements Serializable {

        private final List<GraphViewDesc> insertGraphs = new ArrayList<>();

        public void addGraph(GraphViewDesc graphViewDesc) {
            insertGraphs.add(graphViewDesc);
        }

        public List<GraphViewDesc> getInsertGraphs() {
            return insertGraphs;
        }
    }
}
