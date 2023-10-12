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

import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Description(name = "khop", description = "built-in udga for KHop")
public class KHop implements AlgorithmUserFunction<Object, Integer> {

    private static final String OUTPUT_ID = "id";
    private static final String OUTPUT_K = "k";
    private AlgorithmRuntimeContext<Object, Integer> context;
    private Object srcId;
    private int k = 1;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Integer> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([alpha, [convergence, [max_iteration]]])");
        }
        if (parameters.length > 0) {
            srcId = TypeCastUtil.cast(parameters[0], context.getGraphSchema().getIdType());
        }
        if (parameters.length > 1) {
            k = Integer.parseInt(String.valueOf(parameters[1]));
        }
    }

    @Override
    public void process(RowVertex vertex, Iterator<Integer> messages) {
        List<RowEdge> outEdges = new ArrayList<>(context.loadEdges(EdgeDirection.OUT));
        if (context.getCurrentIterationId() == 1L) {
            if (Objects.equals(srcId, vertex.getId())) {
                sendMessageToNeighbors(outEdges, 1);
                context.updateVertexValue(ObjectRow.create(0));
            } else {
                context.updateVertexValue(ObjectRow.create(Integer.MAX_VALUE));
            }
        } else if (context.getCurrentIterationId() <= k + 1) {
            int currentK = (int) vertex.getValue().getField(0, IntegerType.INSTANCE);
            if (messages.hasNext() && currentK == Integer.MAX_VALUE) {
                Integer currK = messages.next();
                context.updateVertexValue(ObjectRow.create(currK));
                sendMessageToNeighbors(outEdges, currK + 1);
            }
        }
    }

    @Override
    public StructType getOutputType() {
        return new StructType(
            new TableField(OUTPUT_ID, StringType.INSTANCE, false),
            new TableField(OUTPUT_K, IntegerType.INSTANCE, false)
        );
    }

    @Override
    public void finish(RowVertex vertex) {
        int currentK = (int) vertex.getValue().getField(0, IntegerType.INSTANCE);
        if (currentK != Integer.MAX_VALUE) {
            context.take(ObjectRow.create(vertex.getId(), currentK));
        }
    }

    private void sendMessageToNeighbors(List<RowEdge> outEdges, Integer message) {
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
