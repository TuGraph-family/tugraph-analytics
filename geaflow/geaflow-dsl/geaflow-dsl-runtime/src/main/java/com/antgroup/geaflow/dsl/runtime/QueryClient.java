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

package com.antgroup.geaflow.dsl.runtime;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.compile.CompileContext;
import com.antgroup.geaflow.dsl.common.compile.CompileResult;
import com.antgroup.geaflow.dsl.common.compile.QueryCompiler;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.parser.GeaFlowDSLParser;
import com.antgroup.geaflow.dsl.runtime.command.IQueryCommand;
import com.antgroup.geaflow.dsl.runtime.engine.GeaFlowQueryEngine;
import com.antgroup.geaflow.plan.PipelinePlanBuilder;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.plan.visualization.JsonPlanGraphVisualization;
import com.antgroup.geaflow.runtime.pipeline.PipelineContext;
import com.antgroup.geaflow.runtime.pipeline.task.PipelineTaskContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class QueryClient implements QueryCompiler {

    private final GeaFlowDSLParser parser = new GeaFlowDSLParser();

    private final List<QueryCallback> queryCallbacks = new ArrayList<>();

    public QueryClient() {
        registerQueryCallback(InsertGraphMaterialCallback.INSTANCE);
    }

    /**
     * Execute multi-query at once.
     *
     * @param sql The sql script which contains multi-query to execute.
     * @param context The context for query.
     */
    public void executeQuery(String sql, QueryContext context) {
        try {
            List<SqlNode> sqlNodes = parser.parseMultiStatement(sql);
            for (SqlNode sqlNode : sqlNodes) {
                executeQuery(sqlNode, context);
            }
            for (QueryCallback callback : queryCallbacks) {
                callback.onQueryFinish(context);
            }
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException("Error in execute query: \n" + sql, e);
        }
    }

    /**
     * Execute single query.
     *
     * @param sql The sql to execute.
     * @param context The context for executor engine.
     * @return The result for the query.
     */
    public QueryResult executeSingleQuery(String sql, QueryContext context) {

        try {
            SqlNode sqlNode = parser.parseStatement(sql);
            return executeQuery(sqlNode, context);
        } catch (SqlParseException e) {
            throw new GeaFlowDSLException("Error in execute query: \n" + sql, e);
        }
    }


    private QueryResult executeQuery(SqlNode sqlNode, QueryContext context) {
        IQueryCommand command = context.getCommand(sqlNode);
        return command.execute(context);
    }

    public void registerQueryCallback(QueryCallback callback) {
        queryCallbacks.add(callback);
    }

    @Override
    public CompileResult compile(String script, CompileContext context) {
        PipelineContext pipelineContext =
            new PipelineContext("compileTask", new Configuration(context.getConfig()));
        PipelineTaskContext pipelineTaskCxt = new PipelineTaskContext(0L, pipelineContext);
        QueryEngine engineContext = new GeaFlowQueryEngine(pipelineTaskCxt);
        QueryContext queryContext = QueryContext.builder()
            .setEngineContext(engineContext)
            .setCompile(true)
            .build();
        queryContext.putConfigParallelism(context.getParallelisms());
        executeQuery(script, queryContext);

        PipelinePlanBuilder pipelinePlanBuilder = new PipelinePlanBuilder();
        PipelineGraph pipelineGraph = pipelinePlanBuilder.buildPlan(pipelineContext);
        pipelinePlanBuilder.optimizePlan(pipelineContext.getConfig());
        JsonPlanGraphVisualization visualization = new JsonPlanGraphVisualization(pipelineGraph);

        CompileResult compileResult = new CompileResult();
        compileResult.setPhysicPlan(visualization.getJsonPlan());
        compileResult.setSourceTables(queryContext.getReferSourceTables());
        compileResult.setTargetTables(queryContext.getReferTargetTables());
        compileResult.setSourceGraphs(queryContext.getReferSourceGraphs());
        compileResult.setTargetGraphs(queryContext.getReferTargetGraphs());
        return compileResult;
    }
}
