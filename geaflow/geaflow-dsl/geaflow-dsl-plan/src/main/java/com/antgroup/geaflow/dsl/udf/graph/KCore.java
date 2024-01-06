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
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Description(name = "kcore", description = "built-in udga for KCore")
public class KCore implements AlgorithmUserFunction<Object, Integer> {

    private AlgorithmRuntimeContext<Object, Integer> context;
    private int k = 1;

    @Override
    public void init(AlgorithmRuntimeContext<Object, Integer> context, Object[] params) {
        this.context = context;
        if (params.length > 1) {
            throw new IllegalArgumentException(
                "Only support 1 arguments, false arguments "
                    + "usage: func([k]])");
        }
        if (params.length > 0) {
            k = Integer.parseInt(String.valueOf(params[0]));
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Integer> messages) {
        updatedValues.ifPresent(vertex::setValue);
        boolean isFinish = false;
        if (this.context.getCurrentIterationId() == 1) {
            this.context.updateVertexValue(ObjectRow.create(-1));
        } else {
            int currentV = (int) vertex.getValue().getField(0, IntegerType.INSTANCE);
            if (currentV == 0) {
                return;
            }
            int sum = 0;
            while (messages.hasNext()) {
                sum += messages.next();
            }
            if (sum < k) {
                isFinish = true;
                sum = 0;
            }
            context.updateVertexValue(ObjectRow.create(sum));
        }

        if (isFinish) {
            return;
        }

        List<RowEdge> outEdges = this.context.loadEdges(EdgeDirection.OUT);
        for (RowEdge rowEdge : outEdges) {
            context.sendMessage(rowEdge.getTargetId(), 1);
        }

        List<RowEdge> inEdges = this.context.loadEdges(EdgeDirection.IN);
        for (RowEdge rowEdge : inEdges) {
            context.sendMessage(rowEdge.getTargetId(), 1);
        }
        context.sendMessage(vertex.getId(), 0);
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        int component = (int) graphVertex.getValue().getField(0, IntegerType.INSTANCE);
        context.take(ObjectRow.create(graphVertex.getId(), component));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField("v", IntegerType.INSTANCE, false)
        );
    }

}
