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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Description(name = "common_neighbors_for_vertex_set", description = "built-in udga for CommonNeighbors ForVertexSet")
public class CommonNeighborsForVertexSet implements AlgorithmUserFunction<Object, Tuple<Boolean, Boolean>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonNeighborsForVertexSet.class);

    private AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context;
    // 使用String类型来存储ID，避免Integer/Long比较问题
    private final HashSet<String> setA = new HashSet<>();
    private final HashSet<String> setB = new HashSet<>();

    @Override
    public void init(AlgorithmRuntimeContext<Object, Tuple<Boolean, Boolean>> context, Object[] params) {
        this.context = context;
        boolean separatorFound = false;
        for (Object param : params) {
            if (param.toString().isEmpty()) {
                separatorFound = true;
                continue;
            }
            // 将参数ID转换为String类型存储
            if (!separatorFound) {
                setA.add(param.toString());
            } else {
                setB.add(param.toString());
            }
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Boolean, Boolean>> messages) {
        if (context.getCurrentIterationId() == 1L) {
            Tuple<Boolean, Boolean> flag = new Tuple<>(false, false);
            boolean shouldSendMessage = false;

            // 将顶点ID转换为String类型进行比较
            String vertexIdStr = vertex.getId().toString();

            if (setA.contains(vertexIdStr)) {
                flag.setF0(true);
                shouldSendMessage = true;
            }
            if (setB.contains(vertexIdStr)) {
                flag.setF1(true);
                shouldSendMessage = true;
            }

            if (shouldSendMessage) {
                sendMessageToNeighbors(context.loadEdges(EdgeDirection.BOTH), flag);
            }
        } else if (context.getCurrentIterationId() == 2L) {
            Tuple<Boolean, Boolean> received = new Tuple<>(false, false);
            while (messages.hasNext()) {
                Tuple<Boolean, Boolean> message = messages.next();
                if (message.getF0()) {
                    received.setF0(true);
                }
                if (message.getF1()) {
                    received.setF1(true);
                }
            }

            if (received.getF0() && received.getF1()) {
                context.take(ObjectRow.create(vertex.getId()));
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        // 在此算法中，结果是在process方法中流式输出的，
        // 因此finish方法不需要执行任何操作。
    }

    private void sendMessageToNeighbors(List<RowEdge> edges, Tuple<Boolean, Boolean> message) {
        for (RowEdge rowEdge : edges) {
            context.sendMessage(rowEdge.getTargetId(), message);
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("id", graphSchema.getIdType(), false)
        );
    }
}