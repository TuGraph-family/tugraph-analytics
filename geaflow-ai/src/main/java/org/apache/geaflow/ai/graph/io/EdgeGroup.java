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

package org.apache.geaflow.ai.graph.io;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.common.id.IdempotencyKeys;

public class EdgeGroup implements EntityGroup {

    public final EdgeSchema edgeSchema;
    private final List<Edge> edges;
    private final Map<String, List<Integer>> index;
    private final Map<String, List<Integer>> pointIndices;
    private final Set<String> factKeys;

    public EdgeGroup(EdgeSchema edgeSchema, List<Edge> edges) {
        this.edgeSchema = edgeSchema;
        this.edges = edges;
        this.index = new HashMap<>(edges.size());
        this.pointIndices = new HashMap<>(10000);
        this.factKeys = new HashSet<>(edges.size());
        buildIndex();
    }

    private void buildIndex() {
        int index = 0;
        for (Edge e : edges) {
            factKeys.add(factKey(e));
            String key = makeKey(e.getSrcId(), e.getDstId());
            this.index.computeIfAbsent(key, k -> new ArrayList<>()).add(index);
            this.pointIndices.computeIfAbsent(e.getSrcId(), k -> new ArrayList<>()).add(index);
            this.pointIndices.computeIfAbsent(e.getDstId(), k -> new ArrayList<>()).add(index);
            index++;
        }
    }

    public EdgeSchema getEdgeSchema() {
        return edgeSchema;
    }

    public List<Edge> getOutEdges() {
        return edges;
    }

    public List<Edge> getOutEdges(String src) {
        if (pointIndices.get(src) == null) {
            return new ArrayList<>();
        }
        return pointIndices.get(src).stream().map(edges::get)
                .filter(e -> e.getSrcId().equals(src))
                .collect(Collectors.toList());
    }

    public List<Edge> getInEdges(String src) {
        if (pointIndices.get(src) == null) {
            return new ArrayList<>();
        }
        return pointIndices.get(src).stream().map(edges::get)
                .filter(e -> e.getDstId().equals(src))
                .collect(Collectors.toList());
    }

    public List<Edge> getEdge(String src, String dst) {
        List<Integer> edgeIndices = index.get(makeKey(src, dst));
        if (edgeIndices == null) {
            return Collections.emptyList();
        }
        List<Edge> edges = new ArrayList<>(edgeIndices.size());
        for (int i : edgeIndices) {
            edges.add(this.edges.get(i));
        }
        return edges;
    }

    public int addEdge(Edge newEdge) {
        if (newEdge == null || newEdge.getLabel() == null || newEdge.getSrcId() == null
            || newEdge.getDstId() == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED;
        }
        String factKey = factKey(newEdge);
        if (!factKeys.add(factKey)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED;
        }
        this.edges.add(newEdge);
        int index = this.edges.size() - 1;
        String key = makeKey(newEdge.getSrcId(), newEdge.getDstId());
        this.index.computeIfAbsent(key, k -> new ArrayList<>()).add(index);
        this.pointIndices.computeIfAbsent(newEdge.getSrcId(), k -> new ArrayList<>()).add(index);
        this.pointIndices.computeIfAbsent(newEdge.getDstId(), k -> new ArrayList<>()).add(index);
        return ErrorCode.SUCCESS;
    }

    public int removeEdge(Edge edge) {
        if (edge == null || edge.getLabel() == null || edge.getSrcId() == null
            || edge.getDstId() == null) {
            return ErrorCode.GRAPH_ENTITY_GROUP_REMOVE_FAILED;
        }
        String key = makeKey(edge.getSrcId(), edge.getDstId());
        if (!index.containsKey(key)) {
            return ErrorCode.GRAPH_ENTITY_GROUP_REMOVE_FAILED;
        }
        List<Integer> indices = index.get(key);
        List<Integer> deleteOffset = new ArrayList<>();
        for (int offset : indices) {
            Edge existsEdge = this.edges.get(offset);
            boolean needDelete = existsEdge.equals(edge);
            if (needDelete) {
                factKeys.remove(factKey(existsEdge));
                edges.set(offset, null);
                deleteOffset.add(offset);
            }
        }
        for (int del : deleteOffset) {
            Integer offset = del;
            this.index.get(key).remove(offset);
            this.pointIndices.get(edge.getSrcId()).remove(offset);
            this.pointIndices.get(edge.getDstId()).remove(offset);
        }
        return ErrorCode.SUCCESS;
    }

    private String makeKey(String src, String dst) {
        return src + "-" + dst;
    }

    private String factKey(Edge edge) {
        return IdempotencyKeys.edgeFact("legacy-graph-v1", edge.getLabel(), edge.getSrcId(),
            edge.getDstId(), Collections.singletonMap("values", edge.getValues()));
    }
}
