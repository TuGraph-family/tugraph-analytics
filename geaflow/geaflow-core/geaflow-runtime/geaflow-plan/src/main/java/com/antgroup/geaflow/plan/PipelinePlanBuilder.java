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

package com.antgroup.geaflow.plan;

import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.BATCH_NUMBER_PER_CHECKPOINT;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.ENABLE_EXTRA_OPTIMIZE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.ENABLE_EXTRA_OPTIMIZE_SINK;

import com.antgroup.geaflow.api.graph.base.algo.GraphExecAlgo;
import com.antgroup.geaflow.api.graph.materialize.PGraphMaterialize;
import com.antgroup.geaflow.api.pdata.PStreamSink;
import com.antgroup.geaflow.api.pdata.base.PAction;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.context.AbstractPipelineContext;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.model.record.RecordArgs.GraphRecordNames;
import com.antgroup.geaflow.operator.OpArgs;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.IGraphVertexCentricAggOp;
import com.antgroup.geaflow.partitioner.impl.KeyPartitioner;
import com.antgroup.geaflow.pdata.graph.view.compute.ComputeIncGraph;
import com.antgroup.geaflow.pdata.graph.view.materialize.MaterializedIncGraph;
import com.antgroup.geaflow.pdata.graph.view.traversal.TraversalIncGraph;
import com.antgroup.geaflow.pdata.graph.window.compute.ComputeWindowGraph;
import com.antgroup.geaflow.pdata.graph.window.traversal.TraversalWindowGraph;
import com.antgroup.geaflow.pdata.stream.Stream;
import com.antgroup.geaflow.pdata.stream.window.WindowUnionStream;
import com.antgroup.geaflow.plan.graph.AffinityLevel;
import com.antgroup.geaflow.plan.graph.PipelineEdge;
import com.antgroup.geaflow.plan.graph.PipelineGraph;
import com.antgroup.geaflow.plan.graph.PipelineVertex;
import com.antgroup.geaflow.plan.graph.VertexMode;
import com.antgroup.geaflow.plan.graph.VertexType;
import com.antgroup.geaflow.plan.optimizer.PipelineGraphOptimizer;
import com.antgroup.geaflow.plan.optimizer.UnionOptimizer;
import com.antgroup.geaflow.plan.util.DAGValidator;
import com.antgroup.geaflow.plan.visualization.PlanGraphVisualization;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for building and optimizing logical plan.
 */
public class PipelinePlanBuilder implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelinePlanBuilder.class);

    private static final int SINGLE_WINDOW_CHECKPOINT_DURATION = 1;
    public static final int ITERATION_AGG_VERTEX_ID = 0;

    private PipelineGraph pipelineGraph;
    private HashSet<Integer> visitedVIds;
    private int edgeIdGenerator;

    public PipelinePlanBuilder() {
        this.pipelineGraph = new PipelineGraph();
        this.visitedVIds = new HashSet<>();
        this.edgeIdGenerator = 1;
    }

    /**
     * Build the whole plan graph.
     */
    public PipelineGraph buildPlan(AbstractPipelineContext pipelineContext) {
        List<PAction> actions = pipelineContext.getActions();
        if (actions.size() < 1) {
            throw new GeaflowRuntimeException(RuntimeErrors.INST.actionIsEmptyError());
        }
        for (PAction action : actions) {
            visitAction(action);
        }

        // Check the validity of upstream vertex for the current vertex.
        this.pipelineGraph.getVertexMap().values().forEach(pipelineVertex -> {
            LOGGER.info(pipelineVertex.getVertexString());
            DAGValidator.checkVertexValidity(this.pipelineGraph, pipelineVertex, true);
        });

        boolean isSingleWindow = this.pipelineGraph.getSourceVertices().stream().allMatch(v ->
            ((AbstractOperator) v.getOperator()).getOpArgs().getOpType() == OpArgs.OpType.SINGLE_WINDOW_SOURCE);
        if (isSingleWindow) {
            pipelineContext.getConfig().put(BATCH_NUMBER_PER_CHECKPOINT.getKey(),
                String.valueOf(SINGLE_WINDOW_CHECKPOINT_DURATION));
            LOGGER.info("reset checkpoint duration for all single window source pipeline graph");
        }

        return this.pipelineGraph;
    }

    /**
     * Plan optimize.
     */
    public void optimizePlan(Configuration pipelineConfig) {
        List<String> dags = new ArrayList<>();
        List<String> nodeInfos = new ArrayList<>();

        PlanGraphVisualization visualization = new PlanGraphVisualization(this.pipelineGraph);
        String logicalPlan = visualization.getGraphviz();
        dags.add(logicalPlan);
        nodeInfos.add(visualization.getNodeInfo());
        LOGGER.info("logical plan: {}", logicalPlan);

        optimizePipelinePlan(pipelineConfig);
        visualization = new PlanGraphVisualization(this.pipelineGraph);
        String physicalPlan = visualization.getGraphviz();
        dags.add(physicalPlan);
        nodeInfos.add(visualization.getNodeInfo());
        LOGGER.info("physical plan: {}", physicalPlan);
    }

    /**
     * Build plan graph by visiting action node at first.
     */
    private void visitAction(PAction action) {
        int vId = action.getId();
        if (visitedVIds.add(vId)) {
            Stream stream = (Stream) action;
            if (action instanceof PGraphMaterialize) {
                visitMaterializeAction((PGraphMaterialize) action);
            } else {
                PipelineVertex pipelineVertex = new PipelineVertex(vId, stream.getOperator(), stream.getParallelism());
                if (action instanceof PStreamSink) {
                    pipelineVertex.setType(VertexType.sink);
                    pipelineVertex.setVertexMode(VertexMode.append);
                } else {
                    pipelineVertex.setType(VertexType.collect);
                    pipelineVertex.setParallelism(1);
                }

                this.pipelineGraph.addVertex(pipelineVertex);
                Stream input = stream.getInput();
                PipelineEdge pipelineEdge = new PipelineEdge(this.edgeIdGenerator++,
                    input.getId(), vId, input.getPartition(), input.getEncoder());
                this.pipelineGraph.addEdge(pipelineEdge);
                visitNode(stream.getInput());
            }
        }
    }

    /**
     * Visit materialize action.
     */
    private void visitMaterializeAction(PGraphMaterialize materialize) {
        Stream stream = (Stream) materialize;
        PipelineVertex pipelineVertex = new PipelineVertex(
            materialize.getId(), stream.getOperator(), stream.getParallelism());
        pipelineVertex.setType(VertexType.sink);
        pipelineVertex.setVertexMode(VertexMode.append);

        MaterializedIncGraph materializedIncGraph = (MaterializedIncGraph) stream;
        Stream vertexStreamInput = materializedIncGraph.getInput();
        Stream edgeStreamInput = (Stream) materializedIncGraph.getEdges();
        Preconditions.checkArgument(vertexStreamInput != null && edgeStreamInput != null,
            "input vertex and edge stream must be not null");

        PipelineEdge vertexInputEdge = new PipelineEdge(this.edgeIdGenerator++, vertexStreamInput.getId(),
            stream.getId(), vertexStreamInput.getPartition(), vertexStreamInput.getEncoder());
        vertexInputEdge.setEdgeName(GraphRecordNames.Vertex.name());
        this.pipelineGraph.addEdge(vertexInputEdge);
        PipelineEdge edgeInputEdge = new PipelineEdge(this.edgeIdGenerator++, edgeStreamInput.getId(),
            stream.getId(), edgeStreamInput.getPartition(), edgeStreamInput.getEncoder());
        edgeInputEdge.setEdgeName(GraphRecordNames.Edge.name());
        this.pipelineGraph.addEdge(edgeInputEdge);
        this.pipelineGraph.addVertex(pipelineVertex);

        visitNode(vertexStreamInput);
        visitNode(edgeStreamInput);
    }

    /**
     * Visit all plan node and build pipeline graph.
     */
    private void visitNode(Stream stream) {
        int vId = stream.getId();
        if (visitedVIds.add(vId)) {
            PipelineVertex pipelineVertex = new PipelineVertex(vId,
                stream.getOperator(), stream.getParallelism());
            pipelineVertex.setAffinity(AffinityLevel.worker);
            switch (stream.getTransformType()) {
                case StreamSource: {
                    pipelineVertex.setType(VertexType.source);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    break;
                }
                case ContinueGraphCompute: {
                    pipelineVertex.setType(VertexType.inc_iterator);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    ComputeIncGraph pGraphCompute = (ComputeIncGraph) stream;
                    GraphExecAlgo computeType = pGraphCompute.getGraphComputeType();
                    switch (computeType) {
                        case VertexCentric:
                            pipelineVertex.setType(VertexType.inc_vertex_centric);
                            break;
                        default:
                            throw new GeaflowRuntimeException("not support graph compute type, " + computeType);
                    }

                    pipelineVertex.setIterations(pGraphCompute.getMaxIterations());
                    Stream vertexStreamInput = pGraphCompute.getInput();
                    Stream edgeStreamInput = (Stream) pGraphCompute.getEdges();
                    Preconditions.checkArgument(vertexStreamInput != null && edgeStreamInput != null,
                        "input vertex and edge stream must be not null");

                    PipelineEdge vertexInputEdge = new PipelineEdge(this.edgeIdGenerator++, vertexStreamInput.getId(),
                        stream.getId(), vertexStreamInput.getPartition(), vertexStreamInput.getEncoder());
                    vertexInputEdge.setEdgeName(GraphRecordNames.Vertex.name());
                    this.pipelineGraph.addEdge(vertexInputEdge);
                    PipelineEdge edgeInputEdge = new PipelineEdge(this.edgeIdGenerator++, edgeStreamInput.getId(),
                        stream.getId(), edgeStreamInput.getPartition(), edgeStreamInput.getEncoder());
                    edgeInputEdge.setEdgeName(GraphRecordNames.Edge.name());
                    this.pipelineGraph.addEdge(edgeInputEdge);

                    // iteration loop edge
                    PipelineEdge iterationEdge = buildIterationEdge(vId, pGraphCompute.getMsgEncoder());
                    this.pipelineGraph.addEdge(iterationEdge);

                    buildIterationAggVertexAndEdge(pipelineVertex);

                    visitNode(vertexStreamInput);
                    visitNode(edgeStreamInput);
                    break;
                }
                case WindowGraphCompute: {
                    pipelineVertex.setType(VertexType.iterator);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    ComputeWindowGraph pGraphCompute = (ComputeWindowGraph) stream;
                    GraphExecAlgo computeType = pGraphCompute.getGraphComputeType();
                    switch (computeType) {
                        case VertexCentric:
                            pipelineVertex.setType(VertexType.vertex_centric);
                            break;
                        default:
                            throw new GeaflowRuntimeException("not support graph compute type, " + computeType);
                    }

                    pipelineVertex.setIterations(pGraphCompute.getMaxIterations());
                    Stream vertexStreamInput = pGraphCompute.getInput();
                    Stream edgeStreamInput = (Stream) pGraphCompute.getEdges();
                    Preconditions.checkArgument(vertexStreamInput != null && edgeStreamInput != null,
                        "input vertex and edge stream must be not null");

                    PipelineEdge vertexInputEdge = new PipelineEdge(this.edgeIdGenerator++, vertexStreamInput.getId(),
                        stream.getId(), vertexStreamInput.getPartition(), vertexStreamInput.getEncoder());
                    vertexInputEdge.setEdgeName(GraphRecordNames.Vertex.name());
                    this.pipelineGraph.addEdge(vertexInputEdge);
                    PipelineEdge edgeInputEdge = new PipelineEdge(this.edgeIdGenerator++, edgeStreamInput.getId(),
                        stream.getId(), edgeStreamInput.getPartition(), edgeStreamInput.getEncoder());
                    edgeInputEdge.setEdgeName(GraphRecordNames.Edge.name());
                    this.pipelineGraph.addEdge(edgeInputEdge);

                    // iteration loop edge
                    PipelineEdge iterationEdge = buildIterationEdge(vId, pGraphCompute.getMsgEncoder());
                    this.pipelineGraph.addEdge(iterationEdge);

                    buildIterationAggVertexAndEdge(pipelineVertex);

                    visitNode(vertexStreamInput);
                    visitNode(edgeStreamInput);
                    break;
                }
                case WindowGraphTraversal: {
                    pipelineVertex.setType(VertexType.iterator);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    TraversalWindowGraph windowGraph = (TraversalWindowGraph) stream;
                    GraphExecAlgo traversalType = windowGraph.getGraphTraversalType();
                    switch (traversalType) {
                        case VertexCentric:
                            pipelineVertex.setType(VertexType.vertex_centric);
                            break;
                        default:
                            throw new GeaflowRuntimeException("not support graph traversal type, " + traversalType);
                    }

                    pipelineVertex.setIterations(windowGraph.getMaxIterations());
                    Stream vertexStreamInput = windowGraph.getInput();
                    Stream edgeStreamInput = (Stream) windowGraph.getEdges();
                    Preconditions.checkArgument(vertexStreamInput != null && edgeStreamInput != null,
                        "input vertex and edge stream must be not null");

                    PipelineEdge vertexInputEdge = new PipelineEdge(this.edgeIdGenerator++, vertexStreamInput.getId(),
                        stream.getId(), vertexStreamInput.getPartition(), vertexStreamInput.getEncoder());
                    vertexInputEdge.setEdgeName(GraphRecordNames.Vertex.name());
                    this.pipelineGraph.addEdge(vertexInputEdge);
                    PipelineEdge edgeInputEdge = new PipelineEdge(this.edgeIdGenerator++, edgeStreamInput.getId(),
                        stream.getId(), edgeStreamInput.getPartition(), edgeStreamInput.getEncoder());
                    edgeInputEdge.setEdgeName(GraphRecordNames.Edge.name());
                    this.pipelineGraph.addEdge(edgeInputEdge);

                    // Add request input.
                    if (windowGraph.getRequestStream() != null) {
                        Stream requestStreamInput = (Stream) windowGraph.getRequestStream();
                        PipelineEdge requestInputEdge = new PipelineEdge(this.edgeIdGenerator++, requestStreamInput.getId(),
                            stream.getId(), requestStreamInput.getPartition(),
                            requestStreamInput.getEncoder());
                        requestInputEdge.setEdgeName(GraphRecordNames.Request.name());
                        this.pipelineGraph.addEdge(requestInputEdge);
                        visitNode(requestStreamInput);
                    }

                    // iteration loop edge
                    PipelineEdge iterationEdge = buildIterationEdge(vId, windowGraph.getMsgEncoder());
                    this.pipelineGraph.addEdge(iterationEdge);

                    buildIterationAggVertexAndEdge(pipelineVertex);

                    visitNode(vertexStreamInput);
                    visitNode(edgeStreamInput);
                    break;
                }
                case ContinueGraphTraversal: {
                    pipelineVertex.setType(VertexType.inc_iterator);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    TraversalIncGraph windowGraph = (TraversalIncGraph) stream;
                    GraphExecAlgo traversalType = windowGraph.getGraphTraversalType();
                    switch (traversalType) {
                        case VertexCentric:
                            pipelineVertex.setType(VertexType.inc_vertex_centric);
                            break;
                        default:
                            throw new GeaflowRuntimeException("not support graph traversal type, " + traversalType);
                    }

                    pipelineVertex.setIterations(windowGraph.getMaxIterations());
                    Stream vertexStreamInput = windowGraph.getInput();
                    Stream edgeStreamInput = (Stream) windowGraph.getEdges();
                    Preconditions.checkArgument(vertexStreamInput != null && edgeStreamInput != null,
                        "input vertex and edge stream must be not null");

                    // Add vertex input.
                    PipelineEdge vertexInputEdge = new PipelineEdge(this.edgeIdGenerator++, vertexStreamInput.getId(),
                        stream.getId(), vertexStreamInput.getPartition(), vertexStreamInput.getEncoder());
                    vertexInputEdge.setEdgeName(GraphRecordNames.Vertex.name());
                    this.pipelineGraph.addEdge(vertexInputEdge);

                    // Add edge input.
                    PipelineEdge edgeInputEdge = new PipelineEdge(this.edgeIdGenerator++, edgeStreamInput.getId(),
                        stream.getId(), edgeStreamInput.getPartition(), edgeStreamInput.getEncoder());
                    edgeInputEdge.setEdgeName(GraphRecordNames.Edge.name());
                    this.pipelineGraph.addEdge(edgeInputEdge);

                    // Add request input.
                    if (windowGraph.getRequestStream() != null) {
                        Stream requestStreamInput = (Stream) windowGraph.getRequestStream();
                        PipelineEdge requestInputEdge = new PipelineEdge(this.edgeIdGenerator++, requestStreamInput.getId(),
                            stream.getId(), requestStreamInput.getPartition(),
                            requestStreamInput.getEncoder());
                        requestInputEdge.setEdgeName(GraphRecordNames.Request.name());
                        this.pipelineGraph.addEdge(requestInputEdge);
                        visitNode(requestStreamInput);
                    }

                    // Add iteration loop edge
                    PipelineEdge iterationEdge = buildIterationEdge(vId, windowGraph.getMsgEncoder());
                    this.pipelineGraph.addEdge(iterationEdge);

                    buildIterationAggVertexAndEdge(pipelineVertex);

                    visitNode(vertexStreamInput);
                    visitNode(edgeStreamInput);
                    break;
                }
                case StreamTransform: {
                    pipelineVertex.setType(VertexType.process);
                    Stream inputStream = stream.getInput();
                    Preconditions.checkArgument(inputStream != null, "input stream must be not null");

                    PipelineEdge pipelineEdge = new PipelineEdge(this.edgeIdGenerator++, inputStream.getId(), stream.getId(),
                        inputStream.getPartition(), inputStream.getEncoder());
                    this.pipelineGraph.addEdge(pipelineEdge);

                    visitNode(inputStream);
                    break;
                }
                case ContinueStreamCompute: {
                    pipelineVertex.setType(VertexType.inc_process);
                    pipelineVertex.setAffinity(AffinityLevel.worker);
                    Stream inputStream = stream.getInput();
                    Preconditions.checkArgument(inputStream != null, "input stream must be not null");

                    PipelineEdge pipelineEdge = new PipelineEdge(this.edgeIdGenerator++, inputStream.getId(), stream.getId(),
                        inputStream.getPartition(), inputStream.getEncoder());
                    this.pipelineGraph.addEdge(pipelineEdge);

                    visitNode(inputStream);
                    break;
                }
                case StreamUnion: {
                    pipelineVertex.setType(VertexType.union);
                    WindowUnionStream unionStream = (WindowUnionStream) stream;

                    Stream mainInput = stream.getInput();
                    PipelineEdge mainEdge = new PipelineEdge(this.edgeIdGenerator++, mainInput.getId(), unionStream.getId(),
                        mainInput.getPartition(), unionStream.getEncoder());
                    mainEdge.setStreamOrdinal(0);
                    this.pipelineGraph.addEdge(mainEdge);
                    visitNode(mainInput);

                    List<Stream> otherInputs = unionStream.getUnionWindowDataStreamList();
                    for (int index = 0; index < otherInputs.size(); index++) {
                        Stream otherInput = otherInputs.get(index);
                        PipelineEdge rightEdge = new PipelineEdge(this.edgeIdGenerator++, otherInput.getId(),
                            unionStream.getId(), otherInput.getPartition(), otherInput.getEncoder());
                        rightEdge.setStreamOrdinal(index + 1);
                        this.pipelineGraph.addEdge(rightEdge);
                        visitNode(otherInput);
                    }
                    break;
                }
                default:
                    throw new GeaflowRuntimeException("Not supported transform type: " + stream.getTransformType());
            }
            this.pipelineGraph.addVertex(pipelineVertex);
        }
    }

    private PipelineEdge buildIterationEdge(int vid, IEncoder<?> encoder) {
        PipelineEdge iterationEdge = new PipelineEdge(this.edgeIdGenerator++, vid, vid,
            new KeyPartitioner<>(vid), encoder, CollectType.LOOP);
        iterationEdge.setEdgeName(GraphRecordNames.Message.name());
        return iterationEdge;
    }

    private void buildIterationAggVertexAndEdge(PipelineVertex iterationVertex) {
        if (iterationVertex.getOperator() instanceof IGraphVertexCentricAggOp) {
            PipelineVertex aggVertex = new PipelineVertex(ITERATION_AGG_VERTEX_ID,
                iterationVertex.getOperator(), 0);
            aggVertex.setType(VertexType.iteration_aggregation);
            this.pipelineGraph.addVertex(aggVertex);

            PipelineEdge inputEdge = new PipelineEdge(this.edgeIdGenerator++,
                iterationVertex.getVertexId(), ITERATION_AGG_VERTEX_ID,
                new KeyPartitioner<>(iterationVertex.getVertexId()), null, CollectType.RESPONSE);
            inputEdge.setEdgeName(GraphRecordNames.Aggregate.name());
            this.pipelineGraph.addEdge(inputEdge);

            PipelineEdge outputEdge = new PipelineEdge(this.edgeIdGenerator++,
                ITERATION_AGG_VERTEX_ID, iterationVertex.getVertexId(),
                new KeyPartitioner<>(ITERATION_AGG_VERTEX_ID), null, CollectType.RESPONSE);
            outputEdge.setEdgeName(GraphRecordNames.Aggregate.name());
            this.pipelineGraph.addEdge(outputEdge);
        }
    }

    /**
     * Enforce union and chain optimize.
     */
    private void optimizePipelinePlan(Configuration pipelineConfig) {
        if (pipelineConfig.getBoolean(ENABLE_EXTRA_OPTIMIZE)) {
            // Union Optimization.
            boolean isExtraOptimizeSink = pipelineConfig.getBoolean(ENABLE_EXTRA_OPTIMIZE_SINK);
            new UnionOptimizer(isExtraOptimizeSink).optimizePlan(pipelineGraph);
            LOGGER.info("union optimize: {}", new PlanGraphVisualization(pipelineGraph).getGraphviz());
        }
        new PipelineGraphOptimizer().optimizePipelineGraph(pipelineGraph);
    }

}
