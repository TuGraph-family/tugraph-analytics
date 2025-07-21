/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.StringType;
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
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An algorithm to detect cycles in a graph. It handles the engine's internal
 * Object types for vertex IDs and produces engine-compatible BinaryString
 * output.
 */
@Description(name = "path_cycle_detection", description = "Detects cycles of a specified length range starting from a given vertex.")
public class PathCycleDetection implements AlgorithmUserFunction<Object, PathCycleDetection.PathMessage> {

    private AlgorithmRuntimeContext<Object, PathMessage> context;
    private String startVertexIdStr;
    private int minPathLength;
    private int maxPathLength;

    public static class PathMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<Object> path;
        public int currentLength;

        public PathMessage() {
            this.path = new ArrayList<>();
        }

        public PathMessage(List<Object> path, int currentLength) {
            this.path = path;
            this.currentLength = currentLength;
        }
    }

    @Override
    public void init(AlgorithmRuntimeContext<Object, PathMessage> context, Object[] params) {
        this.context = context;
        if (params.length < 3) {
            throw new IllegalArgumentException(
                    "PathCycleDetection requires 3 parameters: startVertexId, minPathLength, maxPathLength");
        }
        this.startVertexIdStr = String.valueOf(params[0]);
        this.minPathLength = Integer.parseInt(String.valueOf(params[1]));
        this.maxPathLength = Integer.parseInt(String.valueOf(params[2]));

        if (minPathLength <= 0 || maxPathLength <= 0 || minPathLength > maxPathLength) {
            throw new IllegalArgumentException("Invalid path length parameters.");
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<PathMessage> messages) {
        Object currentVertexId = vertex.getId();

        if (context.getCurrentIterationId() == 1L) {
            if (String.valueOf(currentVertexId).equals(startVertexIdStr)) {
                List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
                for (RowEdge edge : outEdges) {
                    Object neighborId = edge.getTargetId();
                    List<Object> initialPath = Lists.newArrayList(currentVertexId);
                    context.sendMessage(neighborId, new PathMessage(initialPath, 1));
                }
            }
        } else {
            while (messages.hasNext()) {
                PathMessage receivedMsg = messages.next();
                List<Object> currentPath = receivedMsg.path;
                int currentLength = receivedMsg.currentLength;

                List<Object> newPath = new ArrayList<>(currentPath);
                newPath.add(currentVertexId);

                if (String.valueOf(currentVertexId).equals(startVertexIdStr)) {
                    if (currentLength >= minPathLength && currentLength <= maxPathLength) {
                        String cycleStringJava = newPath.stream().map(String::valueOf)
                                .collect(Collectors.joining("->"));
                        BinaryString cycleStringBinary = BinaryString.fromString(cycleStringJava);
                        context.take(ObjectRow.create(cycleStringBinary, currentLength));
                    }
                    continue;
                }

                if (currentLength >= maxPathLength) {
                    continue;
                }

                List<RowEdge> outEdges = context.loadEdges(EdgeDirection.OUT);
                for (RowEdge edge : outEdges) {
                    Object neighborId = edge.getTargetId();
                    // ** THE FINAL FIX **: Allow revisiting a node only if it's the start node.
                    if (!currentPath.contains(neighborId)
                            || String.valueOf(neighborId).equals(startVertexIdStr)) {
                        context.sendMessage(neighborId, new PathMessage(newPath, currentLength + 1));
                    }
                }

            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
                new TableField("cycle_path", StringType.INSTANCE, false),
                new TableField("length", IntegerType.INSTANCE, false));
    }
}
