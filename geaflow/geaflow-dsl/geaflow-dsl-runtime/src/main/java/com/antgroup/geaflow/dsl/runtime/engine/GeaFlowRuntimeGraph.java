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

package com.antgroup.geaflow.dsl.runtime.engine;

import com.antgroup.geaflow.api.collector.Collector;
import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.base.FlatMapFunction;
import com.antgroup.geaflow.api.function.base.MapFunction;
import com.antgroup.geaflow.api.graph.PGraphWindow;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.DefaultParameterizedRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.rel.GraphAlgorithm;
import com.antgroup.geaflow.dsl.rel.GraphMatch;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RuntimeGraph;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import com.antgroup.geaflow.dsl.runtime.traversal.DagGroupBuilder;
import com.antgroup.geaflow.dsl.runtime.traversal.ExecuteDagGroup;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlan;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlanSet;
import com.antgroup.geaflow.dsl.runtime.traversal.StepLogicalPlanTranslator;
import com.antgroup.geaflow.dsl.runtime.traversal.data.InitParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.TraversalAll;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepOperator;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ConstantStartId;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.ParameterStartId;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepSourceOperator.StartId;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ParameterizedTreePath;
import com.antgroup.geaflow.dsl.runtime.util.IDUtil;
import com.antgroup.geaflow.dsl.schema.GeaFlowGraph;
import com.antgroup.geaflow.model.traversal.ITraversalResponse;
import com.antgroup.geaflow.pipeline.task.IPipelineTaskContext;
import com.antgroup.geaflow.view.graph.GraphViewDesc;
import com.antgroup.geaflow.view.graph.PGraphView;
import com.antgroup.geaflow.view.graph.PIncGraphView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GeaFlowRuntimeGraph implements RuntimeGraph {

    private final QueryContext queryContext;

    private final IPipelineTaskContext context;

    private final GeaFlowGraph graph;

    private final GraphSchema graphSchema;

    private final GraphViewDesc graphViewDesc;

    private final PGraphView<Object, Row, Row> graphView;

    private final StepLogicalPlanSet logicalPlanSet;

    public GeaFlowRuntimeGraph(QueryContext queryContext,
                               PGraphView<Object, Row, Row> graphView,
                               GeaFlowGraph graph,
                               StepLogicalPlanSet logicalPlanSet,
                               GraphViewDesc graphViewDesc) {
        this.queryContext = Objects.requireNonNull(queryContext);
        this.context = ((GeaFlowQueryEngine) queryContext.getEngineContext()).getPipelineContext();
        this.graphView = Objects.requireNonNull(graphView);
        this.graph = Objects.requireNonNull(graph);
        this.graphSchema = graph.getGraphSchema(queryContext.getGqlContext().getTypeFactory());
        this.logicalPlanSet = logicalPlanSet;
        this.graphViewDesc = graphViewDesc;
    }

    public GeaFlowRuntimeGraph(QueryContext queryContext,
                               PGraphView<Object, Row, Row> graphView,
                               GeaFlowGraph graph,
                               GraphViewDesc graphViewDesc) {
        this(queryContext, graphView, graph, planSet(graph, queryContext), graphViewDesc);
    }

    private static StepLogicalPlanSet planSet(GeaFlowGraph graph, QueryContext queryContext) {
        return new StepLogicalPlanSet(graph.getGraphSchema(queryContext.getGqlContext().getTypeFactory()));
    }

    @Override
    public <T> T getPlan() {
        return getPathTable().getPlan();
    }

    @Override
    public List<Path> take() {
        return ArrayUtil.castList(getPathTable().take());
    }

    @Override
    public RuntimeGraph traversal(GraphMatch graphMatch) {
        StepLogicalPlanTranslator planTranslator = new StepLogicalPlanTranslator();
        StepLogicalPlan logicalPlan = planTranslator.translate(graphMatch, logicalPlanSet);
        logicalPlanSet.setMainPlan(logicalPlan);
        return new GeaFlowRuntimeGraph(queryContext, graphView, graph, logicalPlanSet, graphViewDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public RuntimeTable getPathTable() {
        assert logicalPlanSet != null;
        DagGroupBuilder builder = new DagGroupBuilder();
        ExecuteDagGroup executeDagGroup = builder.buildExecuteDagGroup(logicalPlanSet);
        StepOperator<?, ?> mainOp = executeDagGroup.getMainDag().getEntryOperator();
        assert mainOp instanceof StepSourceOperator;
        Set<StartId> startIds = ((StepSourceOperator) mainOp).getStartIds();

        Set<ParameterStartId> parameterStartIds = startIds.stream()
            .filter(id -> id instanceof ParameterStartId)
            .map(id -> (ParameterStartId) id)
            .collect(Collectors.toSet());

        Set<Object> constantStartIds = startIds.stream()
            .filter(id -> id instanceof ConstantStartId)
            .map(id -> ((ConstantStartId) id).getValue())
            .collect(Collectors.toSet());

        PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graph.getName());
        PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graph.getName());

        int maxTraversal = context.getConfig().getInteger(DSLConfigKeys.GEAFLOW_DSL_MAX_TRAVERSAL);
        PWindowStream<ITraversalResponse<ITreePath>> responsePWindow;

        if (graph.isStatic()) { // traversal on static graph.
            vertexStream = vertexStream != null ? vertexStream :
                           queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                               .getPlan();
            edgeStream = edgeStream != null ? edgeStream :
                         queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                             .getPlan();
            PGraphWindow<Object, Row, Row> staticGraph =
                context.buildWindowStreamGraph((PWindowStream) vertexStream, (PWindowStream) edgeStream, graphViewDesc);
            responsePWindow = staticGraphTraversal(staticGraph, parameterStartIds,
                constantStartIds, executeDagGroup, maxTraversal);
        } else { // process dynamic graph
            assert graphView instanceof PIncGraphView : "Illegal graph view";
            queryContext.addMaterializedGraph(graph.getName());
            if (vertexStream == null && edgeStream == null) { // traversal on snapshot of the dynamic graph
                PGraphWindow<Object, Row, Row> staticGraph = graphView.snapshot(graphViewDesc.getCurrentVersion());
                responsePWindow = staticGraphTraversal(staticGraph, parameterStartIds,
                    constantStartIds, executeDagGroup, maxTraversal);
            } else { // traversal on dynamic graph
                vertexStream = vertexStream != null ? vertexStream :
                               queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                                   .getPlan();
                edgeStream = edgeStream != null ? edgeStream :
                             queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                                 .getPlan();

                PIncGraphView<Object, Row, Row> dynamicGraph = graphView.appendGraph((PWindowStream) vertexStream,
                    (PWindowStream) edgeStream);
                responsePWindow = dynamicGraphTraversal(dynamicGraph, parameterStartIds, constantStartIds,
                    executeDagGroup, maxTraversal);
            }
        }
        if (queryContext.getTraversalParallelism() > 0
            && queryContext.getTraversalParallelism() <= graph.getShardCount()) {
            responsePWindow = responsePWindow.withParallelism(queryContext.getTraversalParallelism());
        } else {
            responsePWindow = responsePWindow.withParallelism(graph.getShardCount());
        }
        PWindowStream<Row> resultPWindow = responsePWindow.flatMap(new ResponseToRowFunction())
            .withName(queryContext.createOperatorName("TraversalResponseToRow"));

        return new GeaFlowRuntimeTable(queryContext, context, resultPWindow);
    }

    private PWindowStream<ITraversalResponse<ITreePath>> staticGraphTraversal(
        PGraphWindow<Object, Row, Row> staticGraph,
        Set<ParameterStartId> parameterStartIds,
        Set<Object> constantStartIds,
        ExecuteDagGroup executeDagGroup,
        int maxTraversal) {
        PWindowStream<ITraversalResponse<ITreePath>> responsePWindow;
        if (queryContext.getRequestTable() != null) { // traversal with request
            RuntimeTable requestTable = queryContext.getRequestTable();
            PWindowStream<Row> requestWindowStream = requestTable.getPlan();
            PWindowStream<InitParameterRequest> parameterizedRequest;
            boolean isTraversalAllWithRequest;
            if (parameterStartIds.size() == 1) { // static request table attach the start id
                parameterizedRequest = requestWindowStream.map(
                    new RowToParameterRequestFunction(parameterStartIds.iterator().next()));
                isTraversalAllWithRequest = false;
            } else { // static request table attach all the traversal ids.
                parameterizedRequest = requestWindowStream.map(new RowToParameterRequestFunction(null))
                    .broadcast();
                isTraversalAllWithRequest = true;
            }
            responsePWindow =
                staticGraph.traversal(new GeaFlowStaticVCTraversal(executeDagGroup,
                        maxTraversal, isTraversalAllWithRequest))
                    .start(parameterizedRequest);
        } else if (constantStartIds.size() > 0) { // static request with constant ids.
            responsePWindow =
                staticGraph.traversal(new GeaFlowStaticVCTraversal(executeDagGroup,
                        maxTraversal, false))
                    .start(new ArrayList<>(constantStartIds));
        } else { // traversal all
            responsePWindow =
                staticGraph.traversal(new GeaFlowStaticVCTraversal(executeDagGroup,
                        maxTraversal, false))
                    .start();
        }
        return responsePWindow;
    }

    private PWindowStream<ITraversalResponse<ITreePath>> dynamicGraphTraversal(
        PIncGraphView<Object, Row, Row> dynamicGraph,
        Set<ParameterStartId> parameterStartIds,
        Set<Object> constantStartIds,
        ExecuteDagGroup executeDagGroup,
        int maxTraversal) {
        if (queryContext.getRequestTable() != null) { // dynamic traversal with request
            RuntimeTable requestTable = queryContext.getRequestTable();
            PWindowStream<Row> requestWindowStream = requestTable.getPlan();
            PWindowStream<InitParameterRequest> parameterizedRequest;
            boolean isTraversalAllWithRequest;
            if (parameterStartIds.size() == 1) { // request table attach the start id.
                parameterizedRequest = requestWindowStream.map(
                    new RowToParameterRequestFunction(parameterStartIds.iterator().next()));
                isTraversalAllWithRequest = false;
            } else {
                parameterizedRequest = requestWindowStream.map(new RowToParameterRequestFunction(null))
                    .broadcast();
                isTraversalAllWithRequest = true;
            }
            return dynamicGraph.incrementalTraversal(
                    new GeaFlowDynamicVCTraversal(executeDagGroup, maxTraversal, isTraversalAllWithRequest))
                .start(parameterizedRequest);
        } else if (constantStartIds.size() > 0) { // request with constant ids.
            return dynamicGraph.incrementalTraversal(new GeaFlowDynamicVCTraversal(executeDagGroup,
                    maxTraversal, false))
                .start(new ArrayList<>(constantStartIds));
        } else { // dynamic traversal all
            return dynamicGraph.incrementalTraversal(new GeaFlowDynamicVCTraversal(executeDagGroup,
                    maxTraversal, false))
                .start();
        }
    }

    @Override
    public RuntimeTable runAlgorithm(GraphAlgorithm graphAlgorithm) {
        Class<? extends AlgorithmUserFunction> algorithmUserFunctionClass = graphAlgorithm.getUserFunctionClass();
        AlgorithmUserFunction algorithm;
        try {
            algorithm = algorithmUserFunctionClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new GeaFlowDSLException("Cannot new instance for class: " + algorithmUserFunctionClass.getName(), e);
        }
        int maxTraversal = context.getConfig().getInteger(DSLConfigKeys.GEAFLOW_DSL_MAX_TRAVERSAL);

        PWindowStream<RowVertex> vertexStream = queryContext.getGraphVertexStream(graph.getName());
        PWindowStream<RowEdge> edgeStream = queryContext.getGraphEdgeStream(graph.getName());
        PWindowStream<ITraversalResponse<Row>> responsePWindow;
        if (graph.isStatic()) { // traversal on static graph.
            queryContext.addMaterializedGraph(graph.getName());
            PGraphWindow<Object, Row, Row> staticGraph = context.buildWindowStreamGraph(
                (PWindowStream) vertexStream, (PWindowStream) edgeStream, graphViewDesc);
            responsePWindow = staticGraph.traversal(
                new GeaFlowAlgorithmTraversal(algorithm, maxTraversal, graphAlgorithm.getParams(), graphSchema)).start();
        } else {
            assert graphView instanceof PIncGraphView : "Illegal graph view";
            queryContext.addMaterializedGraph(graph.getName());
            if (vertexStream == null && edgeStream == null) { // traversal on snapshot of the dynamic graph
                PGraphWindow<Object, Row, Row> staticGraph = graphView.snapshot(graphViewDesc.getCurrentVersion());
                responsePWindow = staticGraph.traversal(
                    new GeaFlowAlgorithmTraversal(algorithm, maxTraversal, graphAlgorithm.getParams(), graphSchema)).start();
            } else { // traversal on dynamic graph
                vertexStream = vertexStream != null ? vertexStream :
                               queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                                   .getPlan();
                edgeStream = edgeStream != null ? edgeStream :
                             queryContext.getEngineContext().createRuntimeTable(queryContext, Collections.emptyList())
                                 .getPlan();

                PIncGraphView<Object, Row, Row> dynamicGraph = graphView.appendGraph((PWindowStream) vertexStream,
                    (PWindowStream) edgeStream);
                responsePWindow = dynamicGraph.incrementalTraversal(
                        new GeaFlowAlgorithmDynamicTraversal(algorithm, maxTraversal, graphAlgorithm.getParams(), graphSchema))
                    .start();
            }
        }
        if (queryContext.getTraversalParallelism() > 0
            && queryContext.getTraversalParallelism() <= graph.getShardCount()) {
            responsePWindow = responsePWindow.withParallelism(queryContext.getTraversalParallelism());
        } else {
            responsePWindow = responsePWindow.withParallelism(graph.getShardCount());
        }
        PWindowStream<Row> resultPWindow = responsePWindow.flatMap(
            (FlatMapFunction<ITraversalResponse<Row>, Row>) (value, collector) -> collector.partition(value.getResponse()));
        return new GeaFlowRuntimeTable(queryContext, context, resultPWindow);
    }

    private static class ResponseToRowFunction implements FlatMapFunction<ITraversalResponse<ITreePath>, Row> {

        @Override
        public void flatMap(ITraversalResponse<ITreePath> value, Collector<Row> collector) {
            ITreePath treePath = value.getResponse();
            boolean isParametrizedTreePath = treePath instanceof ParameterizedTreePath;
            List<Path> paths = treePath.toList();
            for (Path path : paths) {
                Row resultRow = path;
                // If traversal with parameter request, we carry the parameter and requestId to the
                // sql function. So that the sql follow the match statement can refer the request parameter.
                if (isParametrizedTreePath) {
                    ParameterizedTreePath parameterizedTreePath = (ParameterizedTreePath) treePath;
                    Object requestId = parameterizedTreePath.getRequestId();
                    Row parameter = parameterizedTreePath.getParameter();
                    resultRow = new DefaultParameterizedRow(path, requestId, parameter);
                }
                collector.partition(resultRow);
            }
        }
    }

    private static class RowToParameterRequestFunction extends RichFunction
        implements MapFunction<Row, InitParameterRequest> {

        private final ParameterStartId startId;

        private int numTasks;

        private int taskIndex;

        private long rowCounter = 0;

        public RowToParameterRequestFunction(ParameterStartId startId) {
            this.startId = startId;
        }

        @Override
        public void open(RuntimeContext runtimeContext) {
            this.numTasks = runtimeContext.getTaskArgs().getParallelism();
            this.taskIndex = runtimeContext.getTaskArgs().getTaskIndex();
        }

        @Override
        public InitParameterRequest map(Row row) {
            long requestId = IDUtil.uniqueId(numTasks, taskIndex, rowCounter);
            if (requestId < 0) {
                throw new GeaFlowDSLException("Request id exceed the Long.MAX, numTasks: "
                    + numTasks + ", taskIndex: " + taskIndex + ", rowCounter: " + rowCounter);
            }
            rowCounter++;
            Object vertexId;
            if (startId != null) {
                vertexId = startId.getIdExpression().evaluate(row);
            } else {
                vertexId = TraversalAll.INSTANCE;
            }
            return new InitParameterRequest(requestId, vertexId, row);
        }

        @Override
        public void close() {

        }
    }
}
