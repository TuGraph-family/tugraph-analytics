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

package com.antgroup.geaflow.dsl.udf.ldbc;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.primitive.DoubleType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Description(name = "bi15_connection", description = "LDBC BI15 Connection Path Algorithm")
public class BIConnectionPathAlgorithm implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;
    private final int propagationIterations = 30;
    private final int pathSearchIterations = 60;
    private final double giganticThreshold = 1000000000.0;
    private final double gigantic = 1000000001.0;

    private final String personType = "Person";
    private final String postType = "Post";
    private final String commentType = "Comment";
    private final String forumType = "Forum";
    private final String knowsType = "knows";
    private final String hasCreatorType = "hasCreator";
    private final String replyOfType = "replyOf";
    private final String containerOfType = "containerOf";

    private RowVertex vertexCache;
    private List<RowEdge> vertexEdgesCache = new ArrayList<>();

    private Object leftSourceVertexId;
    private Object rightSourceVertexId;
    private Long startDate;
    private Long endDate;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] parameters) {
        this.context = context;
        assert parameters.length >= 4 : "Algorithm need source vid parameter.";
        leftSourceVertexId = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        assert leftSourceVertexId != null : "leftSourceVertexId cannot be null for algorithm.";
        rightSourceVertexId = TypeCastUtil.cast(parameters[1], context.getGraphSchema().getIdType());
        assert rightSourceVertexId != null : "rightSourceVertexId cannot be null for algorithm.";
        startDate = (Long) TypeCastUtil.cast(parameters[2], Long.class);
        assert startDate != null : "startDate cannot be null for algorithm.";
        endDate = (Long) TypeCastUtil.cast(parameters[3], Long.class);
        assert endDate != null : "endDate cannot be null for algorithm.";
    }

    @Override
    public void process(RowVertex vertex, Iterator<ObjectRow> messages) {
        //Stage 1 propagation
        if (context.getCurrentIterationId() < propagationIterations) {
            //Send heartbeat messages to keep nodes alive
            if (vertex.getLabel().equals(personType)) {
                context.sendMessage(vertex.getId(), ObjectRow.create(-1.0, 0L));
            }
            switch (vertex.getLabel()) {
                case forumType:
                    long creationDate = (long) vertex.getValue().getField(0, LongType.INSTANCE);
                    if (creationDate >= startDate && creationDate <= endDate) {
                        List<RowEdge> containerOfEdges = loadEdges(vertex, containerOfType, EdgeDirection.OUT);
                        //The forum sends activation message to the post contained
                        for (RowEdge e : containerOfEdges) {
                            context.sendMessage(e.getTargetId(), ObjectRow.create(-1.0, 0L));
                        }
                    }
                    break;
                case postType:
                case commentType:
                    List<RowEdge> hasCreatorEdges = loadEdges(vertex, hasCreatorType, EdgeDirection.OUT);
                    boolean active = false;
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        double score = (double) msg.getField(0, DoubleType.INSTANCE);
                        if (score < 0) {
                            //If an activation message is received from the upstream reply chain,
                            // the Post&Comment nodes transition to the active state.
                            active = true;
                        } else {
                            //Forward the score and the identity of the interacting person to the creator person node.
                            Object interPerson = msg.getField(1, LongType.INSTANCE);
                            for (RowEdge creatorEdge : hasCreatorEdges) {
                                context.sendMessage(creatorEdge.getTargetId(),
                                    ObjectRow.create(score, interPerson));
                            }
                        }
                    }
                    if (active) {
                        //Generate scores and interacting person IDs, and send them downstream.
                        double score = vertex.getLabel().equals(postType) ? 1.0 : 0.5;
                        List<RowEdge> replyOfEdges = loadEdges(vertex, replyOfType, EdgeDirection.IN);
                        if (hasCreatorEdges.size() > 0 && replyOfEdges.size() > 0) {
                            for (RowEdge creatorEdge : hasCreatorEdges) {
                                for (RowEdge replyEdge : replyOfEdges) {
                                    context.sendMessage(replyEdge.getTargetId(),
                                        ObjectRow.create(score, creatorEdge.getTargetId()));
                                }
                            }
                        }
                        //Activate downstream Comment nodes
                        for (RowEdge replyEdge : replyOfEdges) {
                            context.sendMessage(replyEdge.getTargetId(), ObjectRow.create(-1.0, 0L));
                        }
                    }
                    break;
                case personType:
                    List<RowEdge> knowsEdges = loadEdges(vertex, knowsType, EdgeDirection.BOTH);
                    Map<Long, Double> knowsPersonId2InteractionScore = new HashMap<>();
                    if (context.getCurrentIterationId() == 1) {
                        for (RowEdge e : knowsEdges) {
                            knowsPersonId2InteractionScore.put((Long)e.getTargetId(), 0.0);
                        }
                    } else {
                        Map valuesStoreMap = decodeObjectRowAsMap(vertex.getValue(),
                            LongType.INSTANCE, DoubleType.INSTANCE);
                        knowsPersonId2InteractionScore.putAll(valuesStoreMap);
                    }
                    //Aggregate the interaction scores from upstream
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        double score = (double) msg.getField(0, DoubleType.INSTANCE);
                        if (score > 0) {
                            Long interPerson = (long) msg.getField(1, LongType.INSTANCE);
                            if (knowsPersonId2InteractionScore.containsKey(interPerson)) {
                                knowsPersonId2InteractionScore.put(interPerson,
                                    knowsPersonId2InteractionScore.get(interPerson) + score);
                            }
                        }
                    }
                    ObjectRow mapEncodeRow = encodeMapAsObjectRow(knowsPersonId2InteractionScore);
                    context.updateVertexValue(mapEncodeRow);
                    break;
                default:
            }
        } else if (context.getCurrentIterationId() < pathSearchIterations) {
            //Stage 2 Bidirectional dijkstra path searching over Person nodes
            //Send heartbeat messages to keep nodes alive
            if (vertex.getLabel().equals(personType)) {
                context.sendMessage(vertex.getId(), ObjectRow.create(0L, 0.0, gigantic, gigantic));
            }
            if (personType.equals(vertex.getLabel())) {
                Map valuesStoreMap = decodeObjectRowAsMap(vertex.getValue(), LongType.INSTANCE,
                    DoubleType.INSTANCE);
                Map<Long, Double> knowsPersonId2InteractionScore = new HashMap<>(valuesStoreMap);
                boolean valueChanged = false;
                Object vId = vertex.getId();
                double currentDistanceToLeft;
                double currentDistanceToRight;
                if (context.getCurrentIterationId() - propagationIterations == 0L) {
                    currentDistanceToLeft =
                        Objects.equals(vId, leftSourceVertexId) ? 0.0 : gigantic;
                    currentDistanceToRight =
                        Objects.equals(vId, rightSourceVertexId) ? 0.0 : gigantic;
                    valueChanged = true;
                } else {
                    //Merge the message with the interaction scores saved locally, and calculate
                    // the distance value.
                    int mapSize = (int) vertex.getValue().getField(0, IntegerType.INSTANCE);
                    currentDistanceToLeft = (double) vertex.getValue()
                        .getField(1 + 2 * mapSize, DoubleType.INSTANCE);
                    currentDistanceToRight = (double) vertex.getValue()
                        .getField(1 + 2 * mapSize + 1, DoubleType.INSTANCE);
                    //Msg schema: Person.id BIGINT, interactionScore DOUBLE,
                    // leftSourceDistance DOUBLE, rightSourceDistance DOUBLE
                    while (messages.hasNext()) {
                        ObjectRow msg = messages.next();
                        Long personId = (Long) msg.getField(0, LongType.INSTANCE);
                        double interactionScore = (double) msg.getField(1, DoubleType.INSTANCE);
                        double leftDistance = (double) msg.getField(2, DoubleType.INSTANCE);
                        double rightDistance = (double) msg.getField(3, DoubleType.INSTANCE);
                        if (leftDistance >= giganticThreshold
                            && rightDistance >= giganticThreshold) {
                            continue;
                        }
                        if (knowsPersonId2InteractionScore.containsKey(personId)) {
                            interactionScore += knowsPersonId2InteractionScore.get(personId);
                        }
                        double newDeltaDistance = (1.0 / (interactionScore + 1.0));
                        leftDistance += newDeltaDistance;
                        rightDistance += newDeltaDistance;
                        if (leftDistance < currentDistanceToLeft) {
                            currentDistanceToLeft = leftDistance;
                            valueChanged = true;
                        }
                        if (rightDistance < currentDistanceToRight) {
                            currentDistanceToRight = rightDistance;
                            valueChanged = true;
                        }
                    }
                }
                context.updateVertexValue(
                    encodeMapAsObjectRow(knowsPersonId2InteractionScore, currentDistanceToLeft,
                        currentDistanceToRight));
                if (valueChanged) {
                    List<RowEdge> knowsEdges = loadEdges(vertex, knowsType, EdgeDirection.BOTH);
                    for (RowEdge e : knowsEdges) {
                        context.sendMessage(e.getTargetId(),
                            ObjectRow.create(vId, knowsPersonId2InteractionScore.get(
                                (Long)e.getTargetId()), currentDistanceToLeft, currentDistanceToRight));
                    }
                }
            }
        } else {
            int mapSize = (int) vertex.getValue().getField(0, IntegerType.INSTANCE);
            double currentDistanceToLeft = (double) vertex.getValue().getField(
                1 + 2 * mapSize, DoubleType.INSTANCE);
            double currentDistanceToRight = (double) vertex.getValue().getField(
                1 + 2 * mapSize + 1, DoubleType.INSTANCE);
            if (currentDistanceToLeft + currentDistanceToRight < giganticThreshold) {
                context.take(ObjectRow.create(currentDistanceToLeft + currentDistanceToRight));
            }
        }

    }

    @Override
    public StructType getOutputType() {
        return new StructType(
            new TableField("distance", DoubleType.INSTANCE, false)
        );
    }

    private List<RowEdge> loadEdges(RowVertex vertex, String edgeLabel, EdgeDirection direction) {
        if (!vertex.equals(vertexCache)) {
            vertexEdgesCache.clear();
            vertexEdgesCache = context.loadEdges(EdgeDirection.BOTH);
            vertexCache = vertex;
        }
        List<RowEdge> results = new ArrayList<>();
        for (RowEdge e : vertexEdgesCache) {
            if (e.getLabel().equals(edgeLabel)
                && (direction == EdgeDirection.BOTH || e.getDirect() == direction)) {
                results.add(e);
            }
        }
        return  results;
    }

    private static ObjectRow encodeMapAsObjectRow(Map map, Object... originValues) {
        int originValuesLength = originValues != null ? originValues.length : 0;
        Object[] values = new Object[map.keySet().size() * 2 + 1 + originValuesLength];
        values[0] = map.keySet().size();
        int index = 0;
        for (Object key : map.keySet()) {
            values[1 + index * 2] = key;
            values[1 + index * 2 + 1] = map.get(key);
            ++index;
        }
        for (index = 0; index < originValuesLength; index++) {
            values[values.length - originValuesLength + index] = originValues[index];
        }
        return ObjectRow.create((Object[]) values);
    }

    private static Map decodeObjectRowAsMap(Row objectRow, IType keyType, IType valueType) {
        int size = (int) objectRow.getField(0, IntegerType.INSTANCE);
        Map result = new HashMap();
        for (int i = 0; i < size; i++) {
            Object key = objectRow.getField(1 + i * 2, keyType);
            Object value = objectRow.getField(1 + i * 2 + 1, valueType);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public void finish(RowVertex vertex) {
    }
}
