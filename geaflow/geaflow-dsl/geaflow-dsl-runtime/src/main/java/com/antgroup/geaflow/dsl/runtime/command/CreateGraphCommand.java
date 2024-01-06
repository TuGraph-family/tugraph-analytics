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

package com.antgroup.geaflow.dsl.runtime.command;

import com.antgroup.geaflow.dsl.calcite.EdgeRecordType;
import com.antgroup.geaflow.dsl.calcite.VertexRecordType;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.QueryResult;
import com.antgroup.geaflow.dsl.runtime.util.QueryUtil;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.EdgeTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.GraphElementTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph.VertexTable;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlEdgeUsing;
import com.antgroup.geaflow.dsl.sqlnode.SqlVertexUsing;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateGraphCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateGraphCommand.class);

    private final SqlCreateGraph createGraph;

    public CreateGraphCommand(SqlCreateGraph createGraph) {
        this.createGraph = createGraph;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        GQLContext gContext = context.getGqlContext();
        GeaFlowGraph graph = gContext.convertToGraph(createGraph);
        gContext.registerGraph(graph);
        processUsing(graph, context);
        LOGGER.info("Succeed to create graph: {}.", graph);
        return new QueryResult(true);
    }


    private void processUsing(GeaFlowGraph graph, QueryContext context) {

        //Graph first time creation will trigger insert operations
        if (!QueryUtil.isGraphExists(graph, graph.getConfigWithGlobal(context.getGlobalConf()))) {
            //Convert graph construction using tables to equivalent insert statement
            Map<String, String> vertexEdgeName2UsingTableNameMap = graph.getUsingTables();
            List<GraphElementTable> graphElements = new ArrayList<>(graph.getVertexTables());
            graphElements.addAll(graph.getEdgeTables());
            RelDataTypeFactory factory = context.getGqlContext().getTypeFactory();
            for (GraphElementTable tbl : graphElements) {
                if (vertexEdgeName2UsingTableNameMap.containsKey(tbl.getTypeName())) {
                    String usingTable = vertexEdgeName2UsingTableNameMap.get(tbl.getTypeName());
                    Table table = context.getGqlContext().getCatalog().getTable(
                        context.getGqlContext().getCurrentInstance(), usingTable);
                    assert table instanceof GeaFlowTable;
                    SqlCall relatedSqlCall = null;
                    List<SqlNode> createGraphElements =
                        new ArrayList<>(createGraph.getVertices().getList());
                    createGraphElements.addAll(createGraph.getEdges().getList());
                    for (SqlNode node : createGraphElements) {
                        if (node instanceof SqlVertexUsing
                            && ((SqlVertexUsing)node).getName().getSimple().equals(tbl.getTypeName())) {
                            relatedSqlCall = ((SqlVertexUsing)node);
                        } else if (node instanceof SqlEdgeUsing
                            && ((SqlEdgeUsing)node).getName().getSimple().equals(tbl.getTypeName())) {
                            relatedSqlCall = ((SqlEdgeUsing)node);
                        }
                    }
                    assert relatedSqlCall != null;
                    RelRecordType reorderType;
                    if (tbl instanceof VertexTable) {
                        VertexTable vertexTable = (VertexTable) tbl;
                        reorderType = VertexRecordType.createVertexType(
                            vertexTable.getRowType(factory).getFieldList(),
                            ((SqlVertexUsing)relatedSqlCall).getId().getSimple(),
                            factory
                        );
                    } else {
                        EdgeTable edgeTable = (EdgeTable) tbl;
                        SqlEdgeUsing edgeUsing = ((SqlEdgeUsing)relatedSqlCall);
                        reorderType = EdgeRecordType.createEdgeType(
                            edgeTable.getRowType(factory).getFieldList(),
                            edgeUsing.getSourceId().getSimple(),
                            edgeUsing.getTargetId().getSimple(),
                            edgeUsing.getTimeField() == null ? null : edgeUsing.getTimeField().getSimple(),
                            factory
                        );
                    }

                    SqlNode insertSqlNode = createUsingGraphInsert(createGraph.getParserPosition(),
                        graph, tbl.getTypeName(), usingTable, reorderType);
                    QueryCommand insertCommand = new QueryCommand(insertSqlNode);
                    insertCommand.execute(context);
                }
            }
        } else {
            LOGGER.warn("The graph: {} already exists, skip exec using.", graph.getName());
        }
    }

    private static SqlNode createUsingGraphInsert(SqlParserPos pos,
                                                  GeaFlowGraph graph,
                                                  String graphElementName,
                                                  String usingTable,
                                                  RelRecordType reorderType) {
        List<String> elementNames = new ArrayList<>();
        elementNames.add(graph.getName());
        elementNames.add(graphElementName);
        List<SqlIdentifier> columns = reorderType.getFieldList().stream()
            .filter(f -> !f.getName().equals(GraphSchema.LABEL_FIELD_NAME))
            .map(f -> new SqlIdentifier(f.getName(), pos))
            .collect(Collectors.toList());
        return new SqlInsert(
            pos,
            SqlNodeList.EMPTY,
            new SqlIdentifier(elementNames, pos),
            new SqlSelect(pos, null,
                new SqlNodeList(columns, pos),
                new SqlIdentifier(usingTable, pos),
                null, null, null, null, null, null, null),
            null
        );
    }

    @Override
    public SqlNode getSqlNode() {
        return createGraph;
    }
}
