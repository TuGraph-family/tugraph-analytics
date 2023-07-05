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

import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Description(name = "triangle_count", description = "built-in udga for Triangle Count.")
public class TriangleCount implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    private final int maxIteration = 2;

    private String vertexType = null;

    private String edgeType = null;

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        if (params.length >= 1) {
            assert params.length != 2 : "Must include vertex type and edge type";
            assert params[0] instanceof String : "Vertex type parameter should be string.";
            vertexType = (String) params[0];
            assert params[1] instanceof String : "Edge type parameter should be string.";
            edgeType = (String) params[1];
        }
    }

    @Override
    public void process(RowVertex vertex, Iterator<ObjectRow> messages) {
        if (Objects.nonNull(vertexType) && !vertexType.equals(vertex.getLabel())) {
            return;
        }

        if (context.getCurrentIterationId() == 1L) {
            List<RowEdge> rowEdges = context.loadEdges(EdgeDirection.BOTH);
            List<RowEdge> filterEdges = rowEdges;
            if (Objects.nonNull(edgeType)) {
                filterEdges = Lists.newArrayList();
                for (RowEdge rowEdge : rowEdges) {
                    if (edgeType.equals(rowEdge.getLabel())) {
                        filterEdges.add(rowEdge);
                    }
                }
            }

            List<Object> neighborInfo = Lists.newArrayList();
            neighborInfo.add((long) filterEdges.size());
            for (RowEdge rowEdge : filterEdges) {
                neighborInfo.add(rowEdge.getTargetId());
            }
            ObjectRow msg = ObjectRow.create(neighborInfo.toArray());
            for (int i = 1; i < neighborInfo.size(); i++) {
                context.sendMessage(neighborInfo.get(i), msg);
            }
            context.sendMessage(vertex.getId(), ObjectRow.create(0L));
            context.updateVertexValue(msg);
        } else if (context.getCurrentIterationId() <= maxIteration) {
            long count = 0;
            Set<Long> sourceSet = row2Set(vertex.getValue());
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                Set<Long> targetSet = row2Set(msg);
                targetSet.retainAll(sourceSet);
                count += targetSet.size();
            }
            if (count % 2 != 0) {
                throw new GeaFlowDSLException("Triangle count resulted in an invalid number of triangles.");
            }
            context.take(ObjectRow.create(vertex.getId(), count / 2));
        }
    }

    @Override
    public StructType getOutputType() {
        return new StructType(
            new TableField("id", LongType.INSTANCE, false),
            new TableField("count", LongType.INSTANCE, false)
        );
    }

    private Set<Long> row2Set(Row row) {
        long len = (long) row.getField(0, LongType.INSTANCE);
        Object[] ids = new Object[(int) len];
        for (int i = 0; i < len; i++) {
            ids[i] = row.getField(i + 1, LongType.INSTANCE);
        }
        Set<Long> set = Sets.newHashSet();
        for (Object id : ids) {
            set.add((long) id);
        }
        return set;
    }
}
