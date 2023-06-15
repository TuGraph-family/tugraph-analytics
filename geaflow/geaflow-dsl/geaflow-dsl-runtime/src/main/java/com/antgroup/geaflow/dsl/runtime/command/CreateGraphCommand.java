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

import com.antgroup.geaflow.api.function.io.SourceFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.QueryResult;
import com.antgroup.geaflow.dsl.runtime.engine.GeaFlowQueryEngine;
import com.antgroup.geaflow.dsl.runtime.engine.source.FileEdgeTableSource;
import com.antgroup.geaflow.dsl.runtime.engine.source.FileVertexTableSource;
import com.antgroup.geaflow.dsl.runtime.util.SchemaUtil;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import org.apache.calcite.sql.SqlNode;
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

    @SuppressWarnings("unchecked")
    private void processUsing(GeaFlowGraph graph, QueryContext context) {
        if (!graph.isStatic()) { // only static graph has using config.
            return;
        }
        GeaFlowQueryEngine engine = (GeaFlowQueryEngine) context.getEngineContext();
        IWindow<? extends Row> window = AllWindow.getInstance();
        Configuration config = graph.getConfigWithGlobal(new Configuration(engine.getConfig()));

        SourceFunction<RowVertex> vertexSrcFn = new FileVertexTableSource(config, SchemaUtil.getVertexTypes(graph));
        SourceFunction<RowEdge> edgeSrcFn = new FileEdgeTableSource(config, SchemaUtil.getEdgeTypes(graph));
        PWindowStream<RowVertex> vertexWindowStream =
            engine.getPipelineContext().buildSource(vertexSrcFn, (IWindow<RowVertex>) window);
        PWindowStream<RowEdge> edgeWindowStream =
            engine.getPipelineContext().buildSource(edgeSrcFn, (IWindow<RowEdge>) window);

        context.updateVertexAndEdgeToGraph(graph.getName(), graph, vertexWindowStream,
            edgeWindowStream);
    }

    @Override
    public SqlNode getSqlNode() {
        return createGraph;
    }
}
