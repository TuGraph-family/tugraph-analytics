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

import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.compile.GraphInfo;
import com.antgroup.geaflow.dsl.common.compile.TableInfo;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.optimize.OptimizeRules;
import com.antgroup.geaflow.dsl.optimize.RuleGroup;
import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.runtime.command.AlterGraphCommand;
import com.antgroup.geaflow.dsl.runtime.command.CreateFunctionCommand;
import com.antgroup.geaflow.dsl.runtime.command.CreateGraphCommand;
import com.antgroup.geaflow.dsl.runtime.command.CreateTableCommand;
import com.antgroup.geaflow.dsl.runtime.command.CreateViewCommand;
import com.antgroup.geaflow.dsl.runtime.command.DescGraphCommand;
import com.antgroup.geaflow.dsl.runtime.command.DropGraphCommand;
import com.antgroup.geaflow.dsl.runtime.command.IQueryCommand;
import com.antgroup.geaflow.dsl.runtime.command.QueryCommand;
import com.antgroup.geaflow.dsl.runtime.command.SetCommand;
import com.antgroup.geaflow.dsl.runtime.command.UseGraphCommand;
import com.antgroup.geaflow.dsl.runtime.command.UseInstanceCommand;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlAlterGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateFunction;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateView;
import com.antgroup.geaflow.dsl.sqlnode.SqlDescGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlDropGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlUseGraph;
import com.antgroup.geaflow.dsl.sqlnode.SqlUseInstance;
import com.antgroup.geaflow.dsl.util.PathReferenceAnalyzer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;

public class QueryContext {

    private final QueryEngine engineContext;

    private final GQLContext gqlContext;

    private final Map<String, RDataView> viewDataViews = new HashMap<>();

    private boolean isCompile;

    private final List<RuleGroup> optimizeRules = new ArrayList<>(OptimizeRules.RULE_GROUPS);

    private final PathReferenceAnalyzer pathAnalyzer;

    private RuntimeTable requestTable;

    private boolean isIdOnlyRequest;

    private Expression pushFilter;

    private final Map<String, Integer> configParallelisms = new HashMap<>();

    private long opNameCounter = 0L;

    private int traversalParallelism = -1;

    private final Map<String, String> setOptions = new HashMap<>();

    private final Map<String, PWindowStream<RowVertex>> graphVertices = new HashMap<>();

    private final Map<String, PWindowStream<RowEdge>> graphEdges = new HashMap<>();

    private final Map<String, GeaFlowGraph> graphs = new HashMap<>();

    private final Map<String, RuntimeTable> runtimeTables = new HashMap<>();

    private final Map<String, RuntimeGraph> runtimeGraphs = new HashMap<>();

    private final Set<String> materializedGraphs = new HashSet<>();

    private final Set<TableInfo> referSourceTables = new HashSet<>();

    private final Set<TableInfo> referTargetTables = new HashSet<>();

    private final Set<GraphInfo> referSourceGraphs = new HashSet<>();

    private final Set<GraphInfo> referTargetGraphs = new HashSet<>();

    private final List<QueryCallback> queryCallbacks = new ArrayList<>();

    private QueryContext(QueryEngine engineContext, boolean isCompile) {
        this.engineContext = engineContext;
        this.gqlContext = GQLContext.create(new Configuration(engineContext.getConfig()), isCompile);
        this.pathAnalyzer = new PathReferenceAnalyzer(gqlContext);
        this.isCompile = isCompile;
        registerQueryCallback(InsertGraphMaterialCallback.INSTANCE);
    }

    public IQueryCommand getCommand(SqlNode node) {
        SqlKind kind = node.getKind();
        if (!kind.belongsTo(SqlKind.TOP_LEVEL)) {
            throw new IllegalArgumentException("SqlNode is a top level query, current kind is: " + kind);
        }
        switch (kind) {
            case SELECT:
            case GQL_FILTER:
            case GQL_MATCH_PATTERN:
            case GQL_RETURN:
            case INSERT:
            case ORDER_BY:
                return new QueryCommand(node);
            case CREATE_TABLE:
                return new CreateTableCommand((SqlCreateTable) node);
            case CREATE_VIEW:
                return new CreateViewCommand((SqlCreateView) node);
            case SET_OPTION:
                return new SetCommand((SqlSetOption) node);
            case CREATE_GRAPH:
                return new CreateGraphCommand((SqlCreateGraph) node);
            case DROP_GRAPH:
                return new DropGraphCommand((SqlDropGraph) node);
            case DESC_GRAPH:
                return new DescGraphCommand((SqlDescGraph) node);
            case ALTER_GRAPH:
                return new AlterGraphCommand((SqlAlterGraph) node);
            case USE_GRAPH:
                return new UseGraphCommand((SqlUseGraph) node);
            case USE_INSTANCE:
                return new UseInstanceCommand((SqlUseInstance) node);
            case CREATE_FUNCTION:
                return new CreateFunctionCommand((SqlCreateFunction) node);
            default:
                throw new IllegalArgumentException("Not support sql kind: " + kind);
        }
    }

    public GQLContext getGqlContext() {
        return gqlContext;
    }

    public QueryEngine getEngineContext() {
        return engineContext;
    }

    public List<RuleGroup> getLogicalRules() {
        return optimizeRules;
    }

    public boolean isCompile() {
        return isCompile;
    }

    public RDataView getDataViewByViewName(String viewName) {
        return viewDataViews.get(viewName);
    }

    public void putViewDataView(String viewName, RDataView dataView) {
        if (viewDataViews.containsKey(viewName)) {
            throw new IllegalArgumentException("View: " + viewName + " has already registered");
        }
        if (dataView == null) {
            throw new IllegalArgumentException("DataView is null");
        }
        viewDataViews.put(viewName, dataView);
    }

    public boolean setCompile(boolean isCompile) {
        boolean oldValue = this.isCompile;
        this.isCompile = isCompile;
        return oldValue;
    }

    public PathReferenceAnalyzer getPathAnalyzer() {
        return pathAnalyzer;
    }

    public RuntimeTable setRequestTable(RuntimeTable requestTable) {
        RuntimeTable preValue = this.requestTable;
        this.requestTable = requestTable;
        return preValue;
    }

    public boolean setIdOnlyRequest(boolean isIdOnlyRequest) {
        boolean preValue = this.isIdOnlyRequest;
        this.isIdOnlyRequest = isIdOnlyRequest;
        return preValue;
    }

    public RuntimeTable getRequestTable() {
        return requestTable;
    }

    public boolean isIdOnlyRequest() {
        return isIdOnlyRequest;
    }

    public Expression setPushFilter(Expression pushFilter) {
        Expression preFilter = this.pushFilter;
        this.pushFilter = pushFilter;
        return preFilter;
    }

    public Expression getPushFilter() {
        return pushFilter;
    }

    public int getConfigParallelisms(String opName, int defaultParallelism) {
        return configParallelisms.getOrDefault(opName, defaultParallelism);
    }

    public void putConfigParallelism(String opName, int parallelism) {
        configParallelisms.put(opName, parallelism);
    }

    public void putConfigParallelism(Map<String, Integer> parallelisms) {
        configParallelisms.putAll(parallelisms);
    }

    public long getOpNameCount() {
        return opNameCounter++;
    }

    public String createOperatorName(String baseName) {
        return baseName + "-" + getOpNameCount();
    }

    public Map<String, String> getSetOptions() {
        return setOptions;
    }

    public void putSetOption(String key, String value) {
        this.setOptions.put(key, value);
    }

    public void updateVertexAndEdgeToGraph(String graphName,
                                           GeaFlowGraph graph,
                                           PWindowStream<RowVertex> vertexStream,
                                           PWindowStream<RowEdge> edgeStream) {
        graphs.put(graphName, graph);
        graphVertices.put(graphName, vertexStream);
        graphEdges.put(graphName, edgeStream);
    }

    public PWindowStream<RowVertex> getGraphVertexStream(String graphName) {
        return graphVertices.get(graphName);
    }

    public PWindowStream<RowEdge> getGraphEdgeStream(String graphName) {
        return graphEdges.get(graphName);
    }

    public GeaFlowGraph getGraph(String graphName) {
        return graphs.get(graphName);
    }

    public void addMaterializedGraph(String graphName) {
        this.materializedGraphs.add(graphName);
    }

    public Set<String> getNonMaterializedGraphs() {
        Set<String> graphs = new HashSet<>();
        graphs.addAll(graphVertices.keySet());
        graphs.addAll(graphEdges.keySet());

        for (String materializedGraph : materializedGraphs) {
            graphs.remove(materializedGraph);
        }
        return graphs;
    }

    public void addReferSourceTable(GeaFlowTable table) {
        referSourceTables.add(new TableInfo(table.getInstanceName(), table.getName()));
    }

    public void addReferTargetTable(GeaFlowTable table) {
        referTargetTables.add(new TableInfo(table.getInstanceName(), table.getName()));
    }

    public void addReferSourceGraph(GeaFlowGraph graph) {
        referSourceGraphs.add(new GraphInfo(graph.getInstanceName(), graph.getName()));
    }

    public void addReferTargetGraph(GeaFlowGraph graph) {
        referTargetGraphs.add(new GraphInfo(graph.getInstanceName(), graph.getName()));
    }

    public Set<TableInfo> getReferSourceTables() {
        return referSourceTables;
    }

    public Set<TableInfo> getReferTargetTables() {
        return referTargetTables;
    }

    public Set<GraphInfo> getReferSourceGraphs() {
        return referSourceGraphs;
    }

    public Set<GraphInfo> getReferTargetGraphs() {
        return referTargetGraphs;
    }

    public static QueryContextBuilder builder() {
        return new QueryContextBuilder();
    }

    public void putRuntimeTable(String tableName, RuntimeTable table) {
        runtimeTables.put(tableName, table);
    }

    public RuntimeTable getRuntimeTable(String tableName) {
        return runtimeTables.get(tableName);
    }

    public void putRuntimeGraph(String graphName, RuntimeGraph graph) {
        runtimeGraphs.put(graphName, graph);
    }

    public RuntimeGraph getRuntimeGraph(String graphName) {
        return runtimeGraphs.get(graphName);
    }

    public Configuration getGlobalConf() {
        Map<String, String> globalConf = new HashMap<>(engineContext.getConfig());
        globalConf.putAll(setOptions);
        return new Configuration(globalConf);
    }

    public void setTraversalParallelism(int traversalParallelism) {
        this.traversalParallelism = traversalParallelism;
    }

    public int getTraversalParallelism() {
        return this.traversalParallelism;
    }


    public void registerQueryCallback(QueryCallback callback) {
        queryCallbacks.add(callback);
    }

    public void finish() {
        for (QueryCallback callback : queryCallbacks) {
            callback.onQueryFinish(this);
        }
    }

    public static class QueryContextBuilder {

        private QueryEngine engineContext;

        private boolean isCompile;

        private int traversalParallelism = -1;

        public QueryContextBuilder setEngineContext(QueryEngine engineContext) {
            this.engineContext = engineContext;
            return this;
        }

        public QueryContextBuilder setCompile(boolean compile) {
            isCompile = compile;
            return this;
        }

        public QueryContextBuilder setTraversalParallelism(int traversalParallelism) {
            this.traversalParallelism = traversalParallelism;
            return this;
        }

        public QueryContext build() {
            QueryContext context = new QueryContext(engineContext, isCompile);
            context.setTraversalParallelism(traversalParallelism);
            return context;
        }
    }
}
