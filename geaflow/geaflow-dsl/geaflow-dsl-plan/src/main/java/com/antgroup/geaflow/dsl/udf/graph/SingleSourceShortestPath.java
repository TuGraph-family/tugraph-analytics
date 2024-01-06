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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

@Description(name = "sssp", description = "built-in udga Single Source Shortest Path")
public class SingleSourceShortestPath implements AlgorithmUserFunction<Object, Long>,
    IncrementalAlgorithmUserFunction {

    private AlgorithmRuntimeContext<Object, Long> context;
    private Object sourceVertexId;
    private String edgeType = null;
    private String vertexType = null;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Long> context, Object[] parameters) {
        this.context = context;
        assert parameters.length >= 1 : "SSSP algorithm need source vid parameter.";
        sourceVertexId = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        assert sourceVertexId != null : "Source vid cannot be null for SSSP.";
        if (parameters.length >= 2) {
            assert parameters[1] instanceof String : "Edge type parameter should be string.";
            edgeType = (String) parameters[1];
        }
        if (parameters.length >= 3) {
            assert parameters[2] instanceof String : "Vertex type parameter should be string.";
            vertexType = (String) parameters[2];
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Long> messages) {
        if (vertexType != null && !vertex.getLabel().equals(vertexType)) {
            return;
        }
        long newDistance;
        if (Objects.equals(vertex.getId(), sourceVertexId)) {
            newDistance = 0;
        } else {
            newDistance = Long.MAX_VALUE;
        }
        while (messages.hasNext()) {
            long d = messages.next();
            if (d < newDistance) {
                newDistance = d;
            }
        }

        boolean distanceUpdatedForIteration;
        boolean distanceUpdatedForWindow = false;
        if (updatedValues.isPresent()) {
            long oldDistance = (long) updatedValues.get().getField(0, LongType.INSTANCE);
            if (newDistance < oldDistance) {
                distanceUpdatedForIteration = true;
            } else {
                newDistance = oldDistance;
                distanceUpdatedForIteration = false;
            }
            distanceUpdatedForWindow = (Boolean) updatedValues.get().getField(1, BooleanType.INSTANCE);
        } else {
            distanceUpdatedForIteration = true;
        }

        distanceUpdatedForWindow = distanceUpdatedForWindow || distanceUpdatedForIteration;
        context.updateVertexValue(ObjectRow.create(newDistance, distanceUpdatedForWindow));
        long scatterDistance = newDistance == Long.MAX_VALUE ? Long.MAX_VALUE :
                               newDistance + 1;
        if (distanceUpdatedForIteration || context.getCurrentIterationId() <= 1L) {
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                if (edgeType == null || edge.getLabel().equals(edgeType)) {
                    context.sendMessage(edge.getTargetId(), scatterDistance);
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> newValue) {
        if (newValue.isPresent()) {
            Boolean distanceUpdated = (Boolean) newValue.get().getField(1, BooleanType.INSTANCE);
            if (distanceUpdated) {
                long currentDistance = (long) newValue.get().getField(0, LongType.INSTANCE);
                if (currentDistance < Long.MAX_VALUE) {
                    context.take(ObjectRow.create(vertex.getId(), currentDistance));
                }
                context.updateVertexValue(ObjectRow.create(currentDistance, false));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("distance", LongType.INSTANCE, false)
        );
    }
}
