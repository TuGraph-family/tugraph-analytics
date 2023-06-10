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

import com.antgroup.geaflow.cluster.shuffle.LogicalPipelineSliceMeta;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.common.shuffle.DataExchangeMode;
import com.antgroup.geaflow.common.shuffle.ShuffleDescriptor;
import com.antgroup.geaflow.core.graph.ExecutionEdge;
import com.antgroup.geaflow.core.graph.ExecutionTask;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.core.graph.util.ExecutionTaskUtils;
import com.antgroup.geaflow.model.record.RecordArgs;
import com.antgroup.geaflow.partitioner.impl.KeyPartitioner;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.shuffle.InputDescriptor;
import com.antgroup.geaflow.runtime.shuffle.IoDescriptor;
import com.antgroup.geaflow.shuffle.OutputDescriptor;
import com.antgroup.geaflow.shuffle.OutputInfo;
import com.antgroup.geaflow.shuffle.message.Shard;
import com.antgroup.geaflow.shuffle.message.SliceId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IoDescriptorBuilder {

    private static final int SELF_IO_EDGE_ID = 0;

    public static IoDescriptor buildPipelineIoDescriptor(ExecutionTask task,
                                                         ExecutionNodeCycle cycle,
                                                         DataExchangeMode dataOutputExchangeMode,
                                                         Configuration config,
                                                         CycleResultManager resultManager) {

        // Init input descriptor.
        InputDescriptor inputDescriptor;
        // Cycle head will get input data from upstream.
        if (isIteration(cycle.getVertexGroup())) {
            inputDescriptor =
                IoDescriptorBuilder.buildPipelineInputDescriptorFromSelfLoop(task, cycle, cycle.getIterationCount());
        } else {
            if (ExecutionTaskUtils.isCycleHead(task)) {
                inputDescriptor = IoDescriptorBuilder.buildBatchInputDescriptorFromUpstream(task, cycle, resultManager);
            } else {
                inputDescriptor = IoDescriptorBuilder.buildPipelineInputDescriptorFromUpstream(task, cycle);
            }
        }

        // Init output descriptor.
        OutputDescriptor outputDescriptor;
        if (ExecutionTaskUtils.isCycleTail(task)) {
            if (dataOutputExchangeMode == DataExchangeMode.PIPELINE) {
                // Initialize iteration tail output to itself.
                outputDescriptor = IoDescriptorBuilder.buildPipelineOutputDescriptorToSelfLoop(task, cycle);
            } else {
                outputDescriptor = IoDescriptorBuilder.buildBatchOutputDescriptorToDownstream(task, cycle);
            }
        } else {
            outputDescriptor = IoDescriptorBuilder.buildPipelineOutputDescriptorToDownstream(task, cycle);
        }

        IoDescriptor ioDescriptor = new IoDescriptor(inputDescriptor, outputDescriptor);
        return ioDescriptor;
    }

    public static OutputDescriptor buildIterationOutputDescriptor(ExecutionTask task,
                                                                  ExecutionNodeCycle cycle) {
        List<OutputInfo> outputInfoList = new ArrayList<>();

        int vertexId = task.getVertexId();
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(vertexId);

        // 1. Build to downstream.
        for (Integer edgeId : outEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(edge.getTargetId());
            List<Integer> taskIds = tasks.stream().map(e -> e.getTaskId()).collect(Collectors.toList());
            OutputInfo outputInfo = new OutputInfo(edgeId, edge.getEdgeName(), vertexId, ShuffleDescriptor.BATCH);
            // TODO Forward partitioner not write to all output tasks.
            outputInfo.setTargetTaskIndices(taskIds);
            outputInfo.setPartitioner(edge.getPartitioner());
            outputInfo.setEncoder(edge.getEncoder());
            outputInfo.setNumPartitions(tasks.get(0).getNumPartitions());
            outputInfoList.add(outputInfo);
        }

        // 2. Build self loop.
        if (outEdgeIds.size() != 0) {
            ExecutionEdge iterationEdge = vertexGroup.getIterationEdgeMap().get(vertexId);
            List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(vertexId);
            List<Integer> taskIds = tasks.stream().map(ExecutionTask::getTaskId).collect(Collectors.toList());
            OutputInfo outputInfo = new OutputInfo(SELF_IO_EDGE_ID,
                getSelfIoEdgeTag(), task.getVertexId(), ShuffleDescriptor.PIPELINE);
            outputInfo.setTargetTaskIndices(taskIds);
            outputInfo.setPartitioner(new KeyPartitioner<>(iterationEdge.getPartitioner().getOpId()));
            outputInfo.setEncoder(iterationEdge.getEncoder());
            outputInfo.setNumPartitions(task.getMaxParallelism());
            outputInfoList.add(outputInfo);
        }
        OutputDescriptor outputDescriptor = new OutputDescriptor();
        outputDescriptor.setOutputInfoList(outputInfoList);
        return outputDescriptor;
    }

    public static InputDescriptor buildPipelineInputDescriptorFromUpstream(ExecutionTask task,
                                                                           ExecutionNodeCycle cycle) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();

        // 1. Build input descriptor.
        InputDescriptor inputDescriptor = new InputDescriptor();
        Map<Integer, List<Shard>> inputShardResultMap = new HashMap<>();
        Map<Integer, String> streamId2NameMap = new HashMap<>();
        Map<Integer, IEncoder<?>> edgeId2EncoderMap = new HashMap<>();

        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(task.getVertexId());
        for (Integer edgeId : inputEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            List<ExecutionTask> inputTasks = cycle.getVertexIdToTasks().get(edge.getSrcId());

            List<Shard> inputShard = new ArrayList<>(inputTasks.size());
            for (ExecutionTask inputTask : inputTasks) {
                Shard shard = new Shard(edgeId,
                    Arrays.asList(new LogicalPipelineSliceMeta(inputTask.getIndex(), task.getIndex(),
                        cycle.getPipelineId(), edgeId, inputTask.getWorkerInfo().getContainerName())));
                inputShard.add(shard);
            }
            inputShardResultMap.put(edgeId, inputShard);
            streamId2NameMap.put(edgeId, edge.getEdgeName());
            if (edge.getEncoder() != null) {
                edgeId2EncoderMap.put(edgeId, edge.getEncoder());
            }
        }
        inputDescriptor.setInputShardMap(inputShardResultMap);
        inputDescriptor.setStreamId2NameMap(streamId2NameMap);
        inputDescriptor.setEdgeId2EncoderMap(edgeId2EncoderMap);
        inputDescriptor.setShuffleDescriptor(ShuffleDescriptor.PIPELINE);

        return inputDescriptor;
    }

    /**
     * Input from current vertex itself.
     */
    public static InputDescriptor buildPipelineInputDescriptorFromSelfLoop(ExecutionTask executionTask,
                                                                           ExecutionNodeCycle cycle,
                                                                           long iterationId) {
        int vertexId = executionTask.getVertexId();

        Map<Integer, List<Shard>> inputPartitionResultMap = new HashMap<>();
        Map<Integer, String> id2TagMap = new HashMap<>();

        List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(vertexId);
        List<Shard> inputShard = new ArrayList<>(tasks.size());
        for (ExecutionTask task : tasks) {
            Shard shard = new Shard(SELF_IO_EDGE_ID,
                Arrays.asList(new LogicalPipelineSliceMeta(
                    new SliceId(cycle.getPipelineId(), SELF_IO_EDGE_ID, task.getIndex(), executionTask.getIndex()),
                    iterationId, task.getWorkerInfo().getContainerName())));
            inputShard.add(shard);
        }
        inputPartitionResultMap.put(SELF_IO_EDGE_ID, inputShard);
        id2TagMap.put(SELF_IO_EDGE_ID, getSelfIoEdgeTag());

        IEncoder<?> msgEncoder = cycle.getVertexGroup().getIterationEdgeMap().get(vertexId).getEncoder();
        Map<Integer, IEncoder<?>> encoders = Collections.singletonMap(SELF_IO_EDGE_ID, msgEncoder);

        InputDescriptor inputDescriptor = new InputDescriptor();
        inputDescriptor.setInputShardMap(inputPartitionResultMap);
        inputDescriptor.setStreamId2NameMap(id2TagMap);
        inputDescriptor.setEdgeId2EncoderMap(encoders);
        inputDescriptor.setShuffleDescriptor(ShuffleDescriptor.PIPELINE);

        return inputDescriptor;
    }

    public static OutputDescriptor buildPipelineOutputDescriptorToDownstream(ExecutionTask task,
                                                                             ExecutionNodeCycle cycle) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        OutputDescriptor outputDescriptor = new OutputDescriptor();
        List<OutputInfo> outputInfoList = new ArrayList<>();

        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(task.getVertexId());
        for (Integer edgeId : outEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(edge.getTargetId());
            List<Integer> taskIds = tasks.stream().map(e -> e.getTaskId()).collect(Collectors.toList());
            OutputInfo outputInfo = new OutputInfo(edgeId, edge.getEdgeName(), task.getVertexId(), ShuffleDescriptor.PIPELINE);
            outputInfo.setTargetTaskIndices(taskIds);
            outputInfo.setPartitioner(edge.getPartitioner());
            outputInfo.setEncoder(edge.getEncoder());
            outputInfo.setNumPartitions(task.getNumPartitions());
            outputInfoList.add(outputInfo);
        }
        outputDescriptor.setOutputInfoList(outputInfoList);
        return outputDescriptor;
    }

    /**
     * Input from upstream vertices.
     */
    public static InputDescriptor buildBatchInputDescriptorFromUpstream(ExecutionTask executionTask,
                                                                        ExecutionNodeCycle cycle,
                                                                        CycleResultManager resultManager) {

        int vertexId = executionTask.getVertexId();
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();

        InputDescriptor inputDescriptor = new InputDescriptor();
        Map<Integer, List<Shard>> inputPartitionResultMap = new HashMap<>();
        Map<Integer, String> id2TagMap = new HashMap<>();
        Map<Integer, IEncoder<?>> edgeId2EncoderMap = new HashMap<>();

        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(vertexId);

        if (inputEdgeIds != null && !inputEdgeIds.isEmpty()) {
            // TODO Forward partitioner not read all input tasks.
            Map<Integer, List<Shard>> inputs =
                DataExchanger.buildInput(vertexGroup.getVertexMap().get(vertexId), resultManager);

            List<Shard> partitions = inputs.get(executionTask.getIndex());
            // Convert to edgeId to list map.
            for (Shard partition : partitions) {
                int edgeId = partition.getEdgeId();
                if (!inputPartitionResultMap.containsKey(edgeId)) {
                    inputPartitionResultMap.put(edgeId, new ArrayList<>());
                }
                inputPartitionResultMap.get(edgeId).add(partition);
            }

            for (Integer edgeId : inputEdgeIds) {
                ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
                id2TagMap.put(edgeId, edge.getEdgeName());
                if (edge.getEncoder() != null) {
                    edgeId2EncoderMap.put(edgeId, edge.getEncoder());
                }
            }
        }
        inputDescriptor.setInputShardMap(inputPartitionResultMap);
        inputDescriptor.setStreamId2NameMap(id2TagMap);
        inputDescriptor.setEdgeId2EncoderMap(edgeId2EncoderMap);
        inputDescriptor.setShuffleDescriptor(ShuffleDescriptor.BATCH);

        return inputDescriptor;
    }

    /**
     * Input from upstream vertices.
     */
    public static InputDescriptor buildBatchInputDescriptorFromSelfLoop(ExecutionTask executionTask,
                                                                        ExecutionNodeCycle cycle,
                                                                        CycleResultManager resultManager) {

        int vertexId = executionTask.getVertexId();
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();

        InputDescriptor inputDescriptor = new InputDescriptor();
        Map<Integer, List<Shard>> inputPartitionResultMap = new HashMap<>();
        Map<Integer, String> id2TagMap = new HashMap<>();
        Map<Integer, IEncoder<?>> edgeId2EncoderMap = new HashMap<>();

        List<Integer> inputEdgeIds = vertexGroup.getVertexId2InEdgeIds().get(vertexId);

        if (inputEdgeIds != null && !inputEdgeIds.isEmpty()) {
            // TODO Forward partitioner not read all input tasks.
            Map<Integer, List<Shard>> inputs =
                DataExchanger.buildInput(vertexGroup.getVertexMap().get(vertexId), resultManager);

            List<Shard> partitions = inputs.get(executionTask.getIndex());
            // Convert to edgeId to list map.
            for (Shard partition : partitions) {
                int edgeId = partition.getEdgeId();
                if (!inputPartitionResultMap.containsKey(edgeId)) {
                    inputPartitionResultMap.put(edgeId, new ArrayList<>());
                }
                inputPartitionResultMap.get(edgeId).add(partition);
            }

            for (Integer edgeId : inputEdgeIds) {
                ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
                id2TagMap.put(edgeId, edge.getEdgeName());
                if (edge.getEncoder() != null) {
                    edgeId2EncoderMap.put(edgeId, edge.getEncoder());
                }
            }
        }
        inputDescriptor.setInputShardMap(inputPartitionResultMap);
        inputDescriptor.setStreamId2NameMap(id2TagMap);
        inputDescriptor.setEdgeId2EncoderMap(edgeId2EncoderMap);
        inputDescriptor.setShuffleDescriptor(ShuffleDescriptor.BATCH);

        return inputDescriptor;
    }


    public static OutputDescriptor buildBatchOutputDescriptorToDownstream(ExecutionTask executionTask,
                                                                          ExecutionNodeCycle cycle) {

        int vertexId = executionTask.getVertexId();
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();

        // 1. Build output descriptor.
        OutputDescriptor outputDescriptor = new OutputDescriptor();
        List<OutputInfo> outputInfoList = new ArrayList<>();

        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(vertexId);
        for (Integer edgeId : outEdgeIds) {
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(edgeId);
            List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(edge.getTargetId());
            List<Integer> taskIds = tasks.stream().map(e -> e.getTaskId()).collect(Collectors.toList());
            OutputInfo outputInfo = new OutputInfo(edgeId, edge.getEdgeName(), vertexId, ShuffleDescriptor.BATCH);
            // TODO Forward partitioner not write to all output tasks.
            outputInfo.setTargetTaskIndices(taskIds);
            outputInfo.setPartitioner(edge.getPartitioner());
            outputInfo.setEncoder(edge.getEncoder());
            outputInfo.setNumPartitions(executionTask.getNumPartitions());
            outputInfoList.add(outputInfo);
        }
        outputDescriptor.setOutputInfoList(outputInfoList);
        return outputDescriptor;
    }

    public static OutputDescriptor buildPipelineOutputDescriptorToSelfLoop(ExecutionTask executionTask,
                                                                           ExecutionNodeCycle cycle) {
        ExecutionVertexGroup vertexGroup = cycle.getVertexGroup();
        List<Integer> outEdgeIds = vertexGroup.getVertexId2OutEdgeIds().get(executionTask.getVertexId());
        OutputDescriptor outputDescriptor = new OutputDescriptor();
        if (outEdgeIds.size() != 0) {
            // Only support one output edge.
            ExecutionEdge edge = vertexGroup.getEdgeMap().get(outEdgeIds.get(0));

            List<ExecutionTask> tasks = cycle.getVertexIdToTasks().get(executionTask.getVertexId());
            List<Integer> taskIds = tasks.stream().map(e -> e.getTaskId()).collect(Collectors.toList());
            OutputInfo outputInfo = new OutputInfo(SELF_IO_EDGE_ID, getSelfIoEdgeTag(), executionTask.getVertexId(),
                ShuffleDescriptor.PIPELINE);
            outputInfo.setTargetTaskIndices(taskIds);
            outputInfo.setPartitioner(new KeyPartitioner(edge.getPartitioner().getOpId()));
            outputInfo.setEncoder(edge.getEncoder());
            outputInfo.setNumPartitions(executionTask.getMaxParallelism());
            outputDescriptor.setOutputInfoList(Collections.singletonList(outputInfo));
        } else {
            outputDescriptor.setOutputInfoList(null);
        }
        return outputDescriptor;
    }

    private static String getSelfIoEdgeTag() {
        return RecordArgs.GraphRecordNames.Message.name();
    }

    private static boolean isIteration(ExecutionVertexGroup group) {
        if (group.getCycleGroupMeta().isIterative()) {
            return true;
        }
        return false;
    }
}
