/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.dsl.udf.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.geaflow.common.type.primitive.StringType;
import org.apache.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import org.apache.geaflow.dsl.common.algo.AlgorithmUserFunction;
import org.apache.geaflow.dsl.common.data.Row;
import org.apache.geaflow.dsl.common.data.RowEdge;
import org.apache.geaflow.dsl.common.data.RowVertex;
import org.apache.geaflow.dsl.common.data.impl.ObjectRow;
import org.apache.geaflow.dsl.common.function.Description;
import org.apache.geaflow.dsl.common.types.GraphSchema;
import org.apache.geaflow.dsl.common.types.StructType;
import org.apache.geaflow.dsl.common.types.TableField;
import org.apache.geaflow.model.graph.edge.EdgeDirection;

@Description(name = "wcc", description = "built-in udga for WeakConnectedComponents")
public class WeakConnectedComponents implements AlgorithmUserFunction<Object, String> {

    private AlgorithmRuntimeContext<Object, String> context;
    private String keyFieldName = "component";
    private int iteration = 20;

    @Override
    public void init(AlgorithmRuntimeContext<Object, String> context, Object[] parameters) {
        this.context = context;
        if (parameters.length > 2) {
            throw new IllegalArgumentException(
                "Only support zero or more arguments, false arguments "
                    + "usage: func([iteration, [keyFieldName]])");
        }
        if (parameters.length > 0) {
            iteration = Integer.parseInt(String.valueOf(parameters[0]));
        }
        if (parameters.length > 1) {
            keyFieldName = String.valueOf(parameters[1]);
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<String> messages) {
        updatedValues.ifPresent(vertex::setValue);
        List<RowEdge> edges = new ArrayList<>(context.loadEdges(EdgeDirection.BOTH));
        if (context.getCurrentIterationId() == 1L) {
            String initValue = String.valueOf(vertex.getId());
            sendMessageToNeighbors(edges, initValue);
            context.sendMessage(vertex.getId(), String.valueOf(vertex.getId()));
            context.updateVertexValue(ObjectRow.create(initValue));
        } else if (context.getCurrentIterationId() < iteration) {
            String minComponent = messages.next();
            while (messages.hasNext()) {
                String next = messages.next();
                if (next.compareTo(minComponent) < 0) {
                    minComponent = next;
                }
            }
            sendMessageToNeighbors(edges, minComponent);
            context.sendMessage(vertex.getId(), minComponent);
            context.updateVertexValue(ObjectRow.create(minComponent));
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        updatedValues.ifPresent(graphVertex::setValue);
        String component = (String) graphVertex.getValue().getField(0, StringType.INSTANCE);
        context.take(ObjectRow.create(graphVertex.getId(), component));
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false),
            new TableField(keyFieldName, StringType.INSTANCE, false)
        );
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, String message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }
}
