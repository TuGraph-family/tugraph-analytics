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

package com.antgroup.geaflow.runtime.core.scheduler.io;

import com.antgroup.geaflow.cluster.response.IResult;
import com.antgroup.geaflow.cluster.response.ResponseResult;
import com.antgroup.geaflow.cluster.shuffle.LogicalPipelineSliceMeta;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.core.graph.ExecutionEdge;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.io.CollectType;
import com.antgroup.geaflow.io.ResponseOutputDesc;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.processor.impl.AbstractProcessor;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.CollectExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.io.IInputDesc;
import com.antgroup.geaflow.runtime.io.RawDataInputDesc;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import com.antgroup.geaflow.runtime.shuffle.ShardInputDesc;
import com.antgroup.geaflow.shuffle.ForwardOutputDesc;
import com.antgroup.geaflow.shuffle.IOutputDesc;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.antgroup.geaflow.shuffle.message.Shard;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IoDescriptorBuilder {

    public static int COLLECT_DATA_EDGE_ID = 0;

    /**
     * Build pipeline io descriptor for input task.
     */
    public static IoDescriptor buildPipelineIoDescriptor(ExecutionTask task,
                                                         ExecutionNodeCycle cycle,
                                                         CycleResultManager resultManager) {
        InputDescriptor inputDescriptor = buildPipelineInputDescriptor(task, cycle, resultManager);
        OutputDescriptor outputDescriptor = buildPipelineOutputDescriptor(task, cycle);
        IoDescriptor ioDescriptor = new IoDescriptor(inputDescriptor, outputDescriptor);
        return ioDescriptor;
    }

    /**
     * Build pipeline input descriptor.
     */
    private static InputDescriptor buildPipelineInputDescriptor(ExecutionTask task,
                                                                ExecutionNodeCycle cycle,
                                                                CycleResultManager resultManager) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());

        Map<Integer, IInputDesc> inputInfos = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            // Only build forward for pipeline.
            if (edge.getType() != CollectType.FORWARD) {
                continue;
            }
            if (edge.getSrcId() == edge.getTargetId()) {
                continue;
            }
            IInputDesc inputInfo = buildInputInfo(task, edge, cycle, resultManager);
            if (inputInfo != null) {
                inputInfos.put(edgeId, inputInfo);
            }
        }
        return new InputDescriptor(inputInfos);
    }

    /**
     * Build pipeline output descriptor.
     */
    private static OutputDescriptor buildPipelineOutputDescriptor(ExecutionTask task,
                                                                  ExecutionNodeCycle cycle) {
        int vertexId = task.getVertexId();
        if (cycle instanceof CollectExecutionNodeCycle) {
            return new OutputDescriptor(Arrays.asList(buildCollectOutputDesc(task)));
        }
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(vertexId);
        List<IOutputDesc> outputDescs = new ArrayList<>();
        for (Integer edgeId : outEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            IOutputDesc outputDesc = buildOutputDesc(task, edge, cycle);
            outputDescs.add(outputDesc);
        }
        OutputDescriptor outputDescriptor = new OutputDescriptor(outputDescs);
        return outputDescriptor;
    }

    /**
     * Build input descriptor for iteration when do init will build iteration loop input descriptor.
     */
    public static InputDescriptor buildIterationInitInputDescriptor(ExecutionTask task,
                                                                    ExecutionNodeCycle cycle,
                                                                    CycleResultManager resultManager) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());

        Map<Integer, IInputDesc> inputInfos = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            // Only build self loop.
            if (edge.getType() == CollectType.LOOP) {
                IInputDesc inputInfo = buildInputInfo(task, edge, cycle, resultManager);
                if (inputInfo != null) {
                    inputInfos.put(edgeId, inputInfo);
                }
            }
        }
        return new InputDescriptor(inputInfos);
    }

    /**
     * Build iteration input descriptor when execute with RAW_FORWARD input edge.
     */
    public static InputDescriptor buildIterationExecuteInputDescriptor(ExecutionTask task,
                                                                       ExecutionNodeCycle cycle,
                                                                       CycleResultManager resultManager) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());
        Map<Integer, IInputDesc> inputInfos = new HashMap<>();
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            if (edge.getType() == CollectType.RESPONSE) {
                IInputDesc inputInfo = buildInputInfo(task, edge, cycle, resultManager);
                if (inputInfo != null) {
                    inputInfos.put(edgeId, inputInfo);
                }
            }
        }
        return new InputDescriptor(inputInfos);
    }

    /**
     * Build input info for input task and edge.
     */
    protected static IInputDesc buildInputInfo(ExecutionTask task,
                                             ExecutionEdge inputEdge,
                                             ExecutionNodeCycle cycle,
                                             CycleResultManager resultManager) {
        List<ExecutionTask> inputTasks = cycle.getVertexIdToTasks().get(inputEdge.getSrcId());
        int edgeId = inputEdge.getEdgeId();

        switch (inputEdge.getType()) {
            case LOOP:
            case FORWARD:
                int vertexId = task.getVertexId();
                ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
                ShuffleDescriptor shuffleDescriptor;
                List<Shard> inputs = new ArrayList<>(inputTasks.size());
                if (isBatchDataExchange(vertexGroup, inputEdge.getSrcId())) {
                    shuffleDescriptor = ShuffleDescriptor.BATCH;
                    Map<Integer, List<Shard>> taskInputs =
                        DataExchanger.buildInput(vertexGroup.getVertexMap().get(vertexId),inputEdge, resultManager);
                    inputs = taskInputs.get(task.getIndex());
                } else {
                    shuffleDescriptor = ShuffleDescriptor.PIPELINE;
                    // TODO forward partitioner not read all input tasks.
                    for (ExecutionTask inputTask : inputTasks) {
                        Shard shard = new Shard(edgeId,
                            Arrays.asList(new LogicalPipelineSliceMeta(inputTask.getIndex(), task.getIndex(),
                                cycle.getPipelineId(), edgeId, inputTask.getWorkerInfo().getContainerName())));
                        inputs.add(shard);
                    }
                }
                return new ShardInputDesc(edgeId, inputEdge.getEdgeName(), inputs,
                    inputEdge.getEncoder(), shuffleDescriptor);
            case RESPONSE:
                List<IResult> results = resultManager.get(inputEdge.getEdgeId());
                if (results == null) {
                    return null;
                }
                List<Object> dataInput = new ArrayList<>();
                for (IResult result : results) {
                    if (result.getType() != CollectType.RESPONSE) {
                        throw new GeaflowRuntimeException(String.format("edge %s type %s not support handle result %s",
                            inputEdge.getEdgeId(), inputEdge.getType(), result.getType()));
                    }
                    dataInput.addAll(((ResponseResult) result).getResponse());
                }
                return new RawDataInputDesc(edgeId, inputEdge.getEdgeName(), dataInput);
            default:
                throw new GeaflowRuntimeException(String.format("not support build input for edge %s type %s",
                    inputEdge.getEdgeId(), inputEdge.getType()));
        }
    }

    private static IOutputDesc buildCollectOutputDesc(ExecutionTask task) {
        int opId = getCollectOpId((AbstractOperator) ((AbstractProcessor) task.getProcessor()).getOperator());
        ResponseOutputDesc outputDesc = new ResponseOutputDesc(opId,
            COLLECT_DATA_EDGE_ID, CollectType.RESPONSE.name(), CollectType.RESPONSE);
        return outputDesc;

    }

    private static Integer getCollectOpId(AbstractOperator operator) {
        if (operator.getNextOperators().isEmpty()) {
            return operator.getOpArgs().getOpId();
        } else if (operator.getNextOperators().size() == 1) {
            return getCollectOpId((AbstractOperator) operator.getNextOperators().get(0));
        } else {
            throw new GeaflowRuntimeException("not support collect multi-output");
        }
    }

    protected static IOutputDesc buildOutputDesc(ExecutionTask task,
                                                 ExecutionEdge outEdge,
                                                 ExecutionNodeCycle cycle) {

        int vertexId = task.getVertexId();
        switch (outEdge.getType()) {
            case LOOP:
            case FORWARD:
                List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(outEdge.getTargetId());
                List<Integer> taskIds = tasks.stream().map(e -> e.getTaskId()).collect(Collectors.toList());
                ShuffleDescriptor shuffleDescriptor;
                if (isBatchDataExchange(cycle.getVertexGroup(), outEdge.getTargetId())) {
                    shuffleDescriptor = ShuffleDescriptor.BATCH;
                } else {
                    shuffleDescriptor = ShuffleDescriptor.PIPELINE;
                }
                ForwardOutputDesc shuffleOutputDesc = new ForwardOutputDesc(outEdge.getEdgeId(),
                    outEdge.getEdgeName(), vertexId,
                    shuffleDescriptor);
                // TODO forward partitioner not write to all output tasks.
                shuffleOutputDesc.setTargetTaskIndices(taskIds);
                shuffleOutputDesc.setPartitioner(outEdge.getPartitioner());
                shuffleOutputDesc.setEncoder(outEdge.getEncoder());
                if (isIteration(outEdge)) {
                    shuffleOutputDesc.setNumPartitions(task.getMaxParallelism());
                } else {
                    shuffleOutputDesc.setNumPartitions(task.getNumPartitions());
                }
                return shuffleOutputDesc;
            case RESPONSE:
                ResponseOutputDesc outputDesc = new ResponseOutputDesc(outEdge.getPartitioner().getOpId(),
                    outEdge.getEdgeId(), outEdge.getEdgeName(),
                    outEdge.getType());
                return outputDesc;
            default:
                throw new GeaflowRuntimeException(String.format("not support build output for edge %s type %s",
                    outEdge.getEdgeId(), outEdge.getType()));

        }
    }

    private static boolean isIteration(ExecutionEdge edge) {
        if (edge.getSrcId() == edge.getTargetId()) {
            return true;
        }
        return false;
    }

    /**
     * Check whether the input edge is batch exchange mode.
     * If the vertex not in current vertex group, then set the current edge exchange mode to batch.
     */
    private static boolean isBatchDataExchange(ExecutionVertexGroup vertexGroup,
                                               int vertexId) {
        if (!vertexGroup.getVertexMap().containsKey(vertexId)) {
            return true;
        } else {
            return false;
        }
    }
}