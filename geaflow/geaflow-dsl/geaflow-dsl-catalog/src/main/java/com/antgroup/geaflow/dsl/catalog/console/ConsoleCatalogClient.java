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

package com.antgroup.geaflow.dsl.catalog.console;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.GEAFLOW_GW_ENDPOINT;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.dsl.schema.GeaFlowFunction;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.utils.HttpUtil;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsoleCatalogClient {

    private final Map<String, String> headers = new HashMap<>();

    private final String endpoint;

    private final Gson gson;

    public ConsoleCatalogClient(Configuration config) {
        String token = config.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY);
        this.headers.put("geaflow-token", token);
        this.endpoint = config.getString(GEAFLOW_GW_ENDPOINT);
        this.gson = new Gson();
    }

    public void createTable(String instanceName, GeaFlowTable table) {
        String createUrl = endpoint + "/api/instances/" + instanceName + "/tables";
        TableModel tableModel = CatalogUtil.convertToTableModel(table);
        HttpUtil.post(createUrl, gson.toJson(tableModel), headers, Object.class);
    }

    public void createGraph(String instanceName, GeaFlowGraph graph) {
        String createUrl = endpoint + "/api/instances/" + instanceName + "/graphs";
        GraphModel graphModel = CatalogUtil.convertToGraphModel(graph);
        HttpUtil.post(createUrl, gson.toJson(graphModel), headers, Object.class);
    }

    public GeaFlowTable getTable(String instanceName, String tableName) {
        String getUrl = endpoint + "/api/instances/" + instanceName + "/tables/" + tableName;
        TableModel tableModel = HttpUtil.get(getUrl, headers, TableModel.class);
        return CatalogUtil.convertToGeaFlowTable(tableModel, instanceName);
    }

    public GeaFlowGraph getGraph(String instanceName, String graphName) {
        String getUrl = endpoint + "/api/instances/" + instanceName + "/graphs/" + graphName;
        GraphModel graphModel = HttpUtil.get(getUrl, headers, GraphModel.class);
        return CatalogUtil.convertToGeaFlowGraph(graphModel, instanceName);
    }

    public GeaFlowFunction getFunction(String instanceName, String functionName) {
        String getUrl = endpoint + "/api/instances/" + instanceName + "/functions/" + functionName;
        FunctionModel functionModel = HttpUtil.get(getUrl, headers, FunctionModel.class);
        return CatalogUtil.convertToGeaFlowFunction(functionModel);
    }

    public void deleteTable(String instanceName, String tableName) {
        String deleteUrl = endpoint + "/api/instances/" + instanceName + "/tables/" + tableName;
        HttpUtil.delete(deleteUrl, headers);
    }

    public void deleteGraph(String instanceName, String graphName) {
        String deleteUrl = endpoint + "/api/instances/" + instanceName + "/graphs/" + graphName;
        HttpUtil.delete(deleteUrl, headers);
    }

    public Set<String> getTables(String instanceName) {
        String getUrl = endpoint + "/api/instances/" + instanceName + "/tables";
        Set<String> tableNames = new HashSet<>();
        PageList<TableModel> tableModels = HttpUtil.get(getUrl, headers,
            new TypeToken<PageList<TableModel>>(){}.getType());
        List<TableModel> tableModelsList = tableModels.getList();
        for (TableModel tableModel : tableModelsList) {
            tableNames.add(tableModel.getName());
        }
        return tableNames;
    }

    public Set<String> getGraphs(String instanceName) {
        String getUrl = endpoint + "/api/instances/" + instanceName + "/graphs";
        Set<String> graphNames = new HashSet<>();
        PageList<GraphModel> graphModels = HttpUtil.get(getUrl, headers,
            new TypeToken<PageList<GraphModel>>(){}.getType());
        List<GraphModel> graphModelsList = graphModels.getList();
        for (GraphModel graphModel : graphModelsList) {
            graphNames.add(graphModel.getName());
        }
        return graphNames;
    }

    public Set<String> getInstances() {
        String getUrl = endpoint + "/api/instances";
        Set<String> instanceNames = new HashSet<>();
        PageList<InstanceModel> instanceModels = HttpUtil.get(getUrl, headers,
            new TypeToken<PageList<InstanceModel>>(){}.getType());
        List<InstanceModel> instanceModelsList = instanceModels.getList();
        for (InstanceModel instanceModel : instanceModelsList) {
            instanceNames.add(instanceModel.getName());
        }
        return instanceNames;
    }
}
