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
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.runtime.util.SchemaUtil;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import com.antgroup.geaflow.view.graph.PIncGraphView;
import java.util.Set;

public class InsertGraphMaterialCallback implements QueryCallback {

    public static final QueryCallback INSTANCE = new InsertGraphMaterialCallback();

    private InsertGraphMaterialCallback() {

    }

    @Override
    public void onQueryFinish(QueryContext queryContext) {
        Set<String> nonMaterializedGraphs = queryContext.getNonMaterializedGraphs();
        for (String graphName : nonMaterializedGraphs) {
            PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graphName);
            PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graphName);

            GeaFlowGraph graph = queryContext.getGraph(graphName);
            queryContext.updateVertexAndEdgeToGraph(graphName, graph, vertexStream, edgeStream);

            GraphViewDesc graphViewDesc = SchemaUtil.buildGraphViewDesc(graph, queryContext.getGlobalConf());
            IPipelineTaskContext pipelineContext = queryContext.getEngineContext().getContext();
            PGraphView<Object, RowVertex, RowEdge> graphView = pipelineContext.createGraphView(graphViewDesc);
            PIncGraphView<Object, RowVertex, RowEdge> incGraphView =
                graphView.appendGraph((PWindowStream) vertexStream, (PWindowStream) edgeStream);

            incGraphView.materialize();
            queryContext.addMaterializedGraph(graphName);
        }
    }
}
