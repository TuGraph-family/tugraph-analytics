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

package org.apache.geaflow.ai.common.id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.geaflow.ai.common.ErrorCode;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.junit.jupiter.api.Test;

class IdempotencyKeysTest {

    @Test
    void equivalentVertexFactsIgnorePropertyInsertionOrder() {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("name", "Alice");
        first.put("attributes", map("age", 30, "active", true));
        Map<String, Object> reordered = new LinkedHashMap<>();
        reordered.put("attributes", map("active", true, "age", 30));
        reordered.put("name", "Alice");

        String key = IdempotencyKeys.vertexFact("schema-1", "person", "alice", first);
        assertEquals(key, IdempotencyKeys.vertexFact("schema-1", "person", "alice", reordered));
        assertNotEquals(key, IdempotencyKeys.vertexFact("schema-2", "person", "alice", first));
        assertNotEquals(key, IdempotencyKeys.vertexFact("schema-1", "person", "alice",
            Collections.singletonMap("name", "Bob")));
    }

    @Test
    void edgeFactsRetainDirectionAndTypedProperties() {
        Map<String, Object> properties = map("weight", 1, "since", 2020);
        String key = IdempotencyKeys.edgeFact("1", "knows", "alice", "bob", properties);
        assertEquals(key, IdempotencyKeys.edgeFact("1", "knows", "alice", "bob",
            map("since", 2020, "weight", 1)));
        assertNotEquals(key, IdempotencyKeys.edgeFact("1", "knows", "bob", "alice", properties));
        assertNotEquals(key, IdempotencyKeys.edgeFact("1", "knows", "alice", "bob",
            Collections.singletonMap("weight", "1")));
    }

    @Test
    void triplesDistinguishEntitiesFromLiterals() {
        String key = IdempotencyKeys.tripleFact("1", IdempotencyKeys.Term.entity("alice"),
            "knows", IdempotencyKeys.Term.entity("bob"));
        assertEquals(key, IdempotencyKeys.tripleFact("1", IdempotencyKeys.Term.entity("alice"),
            "knows", IdempotencyKeys.Term.entity("bob")));
        assertNotEquals(key, IdempotencyKeys.tripleFact("1", IdempotencyKeys.Term.entity("alice"),
            "knows", IdempotencyKeys.Term.literal("bob", null, null)));
        assertNotEquals(key, IdempotencyKeys.tripleFact("2", IdempotencyKeys.Term.entity("alice"),
            "knows", IdempotencyKeys.Term.entity("bob")));
    }

    @Test
    void chunkAndEvidenceKeysTrackSourceSpan() {
        String first = IdempotencyKeys.chunk("1", "document-A", 0, 5, "hello");
        assertEquals("gf-id:v1:chunk:"
            + "c5e98f44666a7759d06da9f63e3f90f7f59612420ac642cf16b09600440d38c0", first);
        assertEquals(first, IdempotencyKeys.chunk("1", "document-A", 0, 5, "hello"));
        assertNotEquals(first, IdempotencyKeys.chunk("1", "document-A", 6, 11, "hello"));
        assertNotEquals(first, IdempotencyKeys.chunk("2", "document-A", 0, 5, "hello"));
        assertNotEquals(first, IdempotencyKeys.chunk("1", "document-B", 0, 5, "hello"));
        assertNotEquals(first, IdempotencyKeys.chunk("1", "document-A", 0, 5, "world"));

        String fact = IdempotencyKeys.vertexFact("1", "person", "alice",
            Collections.singletonMap("name", "Alice"));
        String evidence = IdempotencyKeys.evidence("1", fact, "document-A", 0, 5);
        assertEquals(evidence, IdempotencyKeys.evidence("1", fact, "document-A", 0, 5));
        assertNotEquals(evidence, IdempotencyKeys.evidence("1", fact, "document-A", 6, 11));
        assertNotEquals(evidence, IdempotencyKeys.evidence("1", fact, "document-B", 0, 5));
    }

    @Test
    void repeatedChunkImportUsesOneGraphVertex() {
        String id = IdempotencyKeys.chunk("1", "document-A", 0, 5, "hello");
        VertexSchema schema = new VertexSchema("chunk", "id", Collections.singletonList("text"));
        VertexGroup group = new VertexGroup(schema, new ArrayList<>());
        assertEquals(ErrorCode.SUCCESS,
            group.addVertex(new Vertex("chunk", id, Collections.singletonList("hello"))));
        assertEquals(ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED,
            group.addVertex(new Vertex("chunk", id, Collections.singletonList("hello"))));
        assertEquals(1, group.getVertices().size());
    }

    @Test
    void repeatedEdgeImportDoesNotAppendADuplicate() {
        EdgeSchema schema = new EdgeSchema("knows", "srcId", "dstId",
            Collections.singletonList("since"));
        EdgeGroup group = new EdgeGroup(schema, new ArrayList<>());
        Edge edge = new Edge("knows", "alice", "bob", Collections.singletonList("2020"));
        assertEquals(ErrorCode.SUCCESS, group.addEdge(edge));
        assertEquals(ErrorCode.GRAPH_ENTITY_GROUP_INSERT_FAILED,
            group.addEdge(new Edge("knows", "alice", "bob", Collections.singletonList("2020"))));
        assertEquals(ErrorCode.SUCCESS,
            group.addEdge(new Edge("knows", "alice", "bob", Collections.singletonList("2021"))));
        assertEquals(ErrorCode.SUCCESS,
            group.addEdge(new Edge("knows", "bob", "alice", Collections.singletonList("2020"))));
        assertEquals(3, group.getOutEdges().size());

        EdgeGroup removable = new EdgeGroup(schema, new ArrayList<>());
        assertEquals(ErrorCode.SUCCESS, removable.addEdge(new Edge("knows", "carol", "dave",
            Collections.singletonList("2020"))));
        assertEquals(ErrorCode.SUCCESS, removable.addEdge(edge));
        assertEquals(ErrorCode.SUCCESS, removable.removeEdge(edge));
        assertEquals(ErrorCode.SUCCESS, removable.addEdge(edge));

        assertEquals(ErrorCode.SUCCESS, group.removeEdge(edge));
        assertEquals(0, group.getEdge("alice", "bob").size());
        assertEquals(ErrorCode.SUCCESS, group.addEdge(edge));
    }

    @Test
    void ambiguousOrUnsupportedInputsFailFast() {
        assertThrows(IllegalArgumentException.class,
            () -> IdempotencyKeys.chunk("1", "document-A", 3, 3, "text"));
        assertThrows(IllegalArgumentException.class,
            () -> IdempotencyKeys.vertexFact("1", "person", "alice",
                Collections.singletonMap("unsupported", new Object())));
    }

    private static Map<String, Object> map(String firstKey, Object firstValue,
                                           String secondKey, Object secondValue) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(firstKey, firstValue);
        map.put(secondKey, secondValue);
        return map;
    }
}
