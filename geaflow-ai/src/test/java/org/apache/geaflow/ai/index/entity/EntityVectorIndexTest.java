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

package org.apache.geaflow.ai.index.entity;

import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.geaflow.ai.GraphMemoryServer;
import org.apache.geaflow.ai.common.model.EmbeddingService;
import org.apache.geaflow.ai.common.model.ModelConfig;
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.LocalMemoryGraphAccessor;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.EdgeGroup;
import org.apache.geaflow.ai.graph.io.EdgeSchema;
import org.apache.geaflow.ai.graph.io.EntityGroup;
import org.apache.geaflow.ai.graph.io.GraphSchema;
import org.apache.geaflow.ai.graph.io.MemoryGraph;
import org.apache.geaflow.ai.graph.io.Vertex;
import org.apache.geaflow.ai.graph.io.VertexGroup;
import org.apache.geaflow.ai.graph.io.VertexSchema;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.operator.EmbeddingOperator;
import org.apache.geaflow.ai.search.VectorSearch;
import org.apache.geaflow.ai.subgraph.SubGraph;
import org.apache.geaflow.ai.verbalization.VerbalizationFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

class EntityVectorIndexTest {

    private static final Instant CREATED_AT = Instant.parse("2026-01-01T00:00:00Z");

    private final EntityVectorMetadata metadata =
        new EntityVectorMetadata("test-model", 2, "index-1", "vectors-1", CREATED_AT);
    private final Map<GraphEntity, List<IVector>> embeddings = new HashMap<>();
    private final AtomicInteger reads = new AtomicInteger();
    private LocalMemoryGraphAccessor graph;
    private GraphVertex first;
    private GraphVertex second;
    private GraphVertex empty;
    private GraphEdge edge;
    private IndexStore delegate;
    private EntityVectorIndex index;

    @BeforeEach
    void setUp() {
        GraphSchema schema = new GraphSchema();
        schema.setName("entity-vectors");
        VertexSchema vertices = new VertexSchema("node", "id", Collections.emptyList());
        EdgeSchema edges = new EdgeSchema("rel", "src", "dst", Collections.emptyList());
        schema.addVertex(vertices);
        schema.addEdge(edges);
        Map<String, EntityGroup> groups = new HashMap<>();
        groups.put("node", new VertexGroup(vertices, Arrays.asList(
            new Vertex("node", "a", Collections.emptyList()),
            new Vertex("node", "b", Collections.emptyList()),
            new Vertex("node", "c", Collections.emptyList()))));
        groups.put("rel", new EdgeGroup(edges, Collections.singletonList(
            new Edge("rel", "a", "b", Collections.emptyList()))));
        graph = new LocalMemoryGraphAccessor(new MemoryGraph(schema, groups));
        first = graph.getVertex("node", "a");
        second = graph.getVertex("node", "b");
        empty = graph.getVertex("node", "c");
        edge = graph.getEdge("rel", "a", "b").get(0);
        embeddings.put(first, Arrays.asList(vector(1, 0), vector(0, 1)));
        embeddings.put(second, Collections.singletonList(vector(0.8, 0.6)));
        embeddings.put(edge, Collections.singletonList(vector(1, 0)));
        delegate = entity -> {
            reads.incrementAndGet();
            return embeddings.getOrDefault(entity, Collections.emptyList());
        };
        index = new EntityVectorIndex(delegate, metadata);
    }

    @Test
    void identitiesDistinguishKindsLabelsAndComponentBoundaries() {
        GraphVertex left = new GraphVertex(new Vertex("bc", "a", Collections.emptyList()));
        GraphVertex right = new GraphVertex(new Vertex("c", "ab", Collections.emptyList()));
        assertEquals(ModelUtils.getGraphEntityKey(left), ModelUtils.getGraphEntityKey(right));
        assertNotEquals(EntityReference.of(left), EntityReference.of(right));
        GraphEdge sameLabel = new GraphEdge(new Edge("node", "a", "b", Collections.emptyList()));
        assertEquals(EntityType.VERTEX, EntityReference.of(first).getEntityType());
        assertEquals(EntityType.EDGE, EntityReference.of(sameLabel).getEntityType());
        assertNotEquals(EntityReference.of(first), EntityReference.of(sameLabel));
        assertEquals(EntityReference.of(first),
            EntityReference.of(new GraphVertex(new Vertex("node", "a", Arrays.asList("changed")))));
        assertNotEquals(EntityReference.of(edge), EntityReference.of(
            new GraphEdge(new Edge("rel", "b", "a", Collections.emptyList()))));
    }

    @Test
    void typedLookupRetainsEverySegmentAndReturnsSnapshots() {
        List<EntityVectorRecord> records = index.getEntityVectors(first);
        assertEquals(2, records.size());
        assertNotEquals(records.get(0).getVectorId(), records.get(1).getVectorId());
        assertEquals(1, records.get(1).getOrdinal());
        assertSame(metadata, records.get(0).getMetadata());
        assertEquals("cosine", metadata.getDistance());
        assertEquals(1, metadata.getFormatVersion());
        assertEquals(CREATED_AT, metadata.getCreatedAt());
        double[] values = records.get(0).getEmbedding();
        values[0] = 99;
        assertArrayEquals(new double[] {1, 0}, records.get(0).getEmbedding());
        assertThrows(UnsupportedOperationException.class, () -> records.clear());
        assertTrue(index.getEntityVectors(empty).isEmpty());
        assertTrue(index.getEntityVectors(null).isEmpty());
        assertEquals(records.get(0).getVectorId(), index.getEntityVectors(first).get(0).getVectorId());
    }

    @Test
    void embeddingExportDoesNotExposeTheBackingArray() {
        EmbeddingVector vector = vector(1, 0);
        vector.getValues()[0] = 99;
        assertEquals(1.0, vector.match(vector(1, 0)), 1e-12);
    }

    @Test
    void searchesBothKindsAndAggregatesSegmentsBeforeTopK() {
        VectorSearch query = query("test-model", vector(1, 0), vector(0, 1));
        List<EntityVectorHit> hits = index.search(graph, query, 10, 0.5, null);
        assertEquals(3, hits.size());
        assertEquals(1.0, hits.get(0).getScore(), 1e-12);
        assertEquals(1.0, hits.get(1).getScore(), 1e-12);
        assertEquals(0.8, hits.get(2).getScore(), 1e-12);
        assertEquals(2, index.search(graph, query, 10, 0.5, EntityType.VERTEX).size());
        List<EntityVectorHit> edgeHits = index.search(graph, query, 10, 0.5, EntityType.EDGE);
        assertEquals(1, edgeHits.size(), "edges scanned from both endpoints must be deduplicated");
        assertEquals(EntityReference.of(edge), edgeHits.get(0).getEntity());
        assertEquals(hits.get(0).getEntity(),
            index.search(graph, query, 1, 0.5, null).get(0).getEntity());
        assertTrue(index.search(graph, query, 10, 1.0, null).isEmpty(),
            "the threshold comparison must remain strict");
    }

    @Test
    void selectsTheWinningSegmentWithoutReturningDuplicateEntities() {
        List<EntityVectorHit> hits = index.search(graph,
            query("test-model", vector(0, 1)), 1, 0.5, null);
        assertEquals(1, hits.size());
        assertEquals(EntityReference.of(first), hits.get(0).getEntity());
        assertEquals(1, hits.get(0).getRecord().getOrdinal());
    }

    @Test
    void rejectsQueryModelBeforeReadingCandidatesEvenWithEqualDimensions() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> index.search(graph, query("other-model", vector(1, 0)), 1, 0.5, null));
        assertTrue(error.getMessage().contains("model mismatch"));
        assertEquals(0, reads.get());
        assertThrows(IllegalArgumentException.class,
            () -> index.search(graph, query(null, vector(1, 0)), 1, 0.5, null));
        assertEquals(0, reads.get());
    }

    @Test
    void rejectsQueryDimensionAndNonFiniteValuesEvenForAnEmptyIndex() {
        embeddings.clear();
        assertThrows(IllegalArgumentException.class,
            () -> index.search(graph, query("test-model", vector(1)), 1, 0.5, null));
        assertThrows(IllegalArgumentException.class,
            () -> index.search(graph, query("test-model", vector(Double.NaN, 0)), 1, 0.5, null));
        assertEquals(0, reads.get());
    }

    @Test
    void rejectsInvalidStoredVectorsBeforeScoringRegardlessOfKindFilter() {
        embeddings.put(edge, Collections.singletonList(vector(1, 0, 0)));
        assertThrows(IllegalArgumentException.class, () -> index.search(graph,
            query("test-model", vector(1, 0)), 1, 0.5, EntityType.VERTEX));
        embeddings.put(edge, Collections.singletonList(vector(Double.POSITIVE_INFINITY, 0)));
        assertThrows(IllegalArgumentException.class,
            () -> new EmbeddingOperator(graph, index).apply(null,
                query("test-model", vector(1, 0))));
    }

    @Test
    void unboundWrapperKeepsLegacyLookupButRejectsTypedSearch() {
        EntityVectorIndex unbound = new EntityVectorIndex(delegate);
        assertSame(embeddings.get(first), unbound.getEntityIndex(first));
        assertTrue(unbound.getEntityIndex(null).isEmpty());
        assertEquals(results(delegate, query(null, vector(1, 0))),
            results(unbound, query(null, vector(1, 0))));
        assertThrows(IllegalStateException.class, () -> unbound.getEntityVectors(first));
        assertThrows(IllegalStateException.class, () -> unbound.search(graph,
            query("test-model", vector(1, 0)), 1, 0.5, null));
    }

    @Test
    void boundOperatorPreservesGlobalAndExpansionResults() {
        VectorSearch query = query("test-model", vector(1, 0));
        List<List<GraphEntity>> legacy = results(delegate, query);
        assertEquals(legacy, results(index, query));
        assertFalse(legacy.stream().flatMap(List::stream).anyMatch(GraphEdge.class::isInstance));
        List<SubGraph> oldResult = new EmbeddingOperator(graph, delegate).apply(
            Collections.singletonList(new SubGraph().addVertex(first)), query);
        List<SubGraph> newResult = new EmbeddingOperator(graph, index).apply(
            Collections.singletonList(new SubGraph().addVertex(first)), query);
        assertEquals(oldResult.get(0).getGraphEntityList(), newResult.get(0).getGraphEntityList());
        assertTrue(newResult.get(0).getGraphEntityList().contains(edge));
        assertThrows(IllegalArgumentException.class,
            () -> results(index, query("wrong-model", vector(1, 0))));
        assertThrows(IllegalArgumentException.class,
            () -> results(index, query(null, vector(1, 0))));
    }

    @Test
    void graphMemoryServerDispatchesTheWrapperAndValidatesBoundQueries() {
        GraphMemoryServer server = new GraphMemoryServer();
        server.addGraphAccessor(graph);
        server.addIndexStore(index);
        VectorSearch query = new VectorSearch("memory", server.createSession());
        query.setEmbeddingModel("test-model");
        query.addVector(vector(1, 0));
        server.search(query);
        assertTrue(server.getSessionEntities(query.getSessionId()).contains(first));
        query.setEmbeddingModel("wrong-model");
        assertThrows(IllegalArgumentException.class, () -> server.search(query));
    }

    @Test
    void legacyJsonlReloadPreservesRecordsAndOperatorResults(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("embeddings.jsonl");
        List<String> lines = new ArrayList<>();
        Gson gson = new Gson();
        for (Map.Entry<GraphEntity, List<IVector>> entry : embeddings.entrySet()) {
            for (IVector vector : entry.getValue()) {
                lines.add(gson.toJson(new EmbeddingService.EmbeddingResult(
                    ModelUtils.getGraphEntityKey(entry.getKey()),
                    ((EmbeddingVector) vector).getValues())));
            }
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
        byte[] original = Files.readAllBytes(file);
        EmbeddingIndexStore legacy = load(file);
        EntityVectorIndex wrapped = new EntityVectorIndex(legacy, metadata);
        assertEquals(results(legacy, query("test-model", vector(1, 0))),
            results(wrapped, query("test-model", vector(1, 0))));
        List<EntityVectorRecord> before = wrapped.getEntityVectors(first);
        List<EntityVectorRecord> after =
            new EntityVectorIndex(load(file), metadata).getEntityVectors(first);
        assertEquals(before.size(), after.size());
        for (int i = 0; i < before.size(); i++) {
            assertEquals(before.get(i).getVectorId(), after.get(i).getVectorId());
            assertArrayEquals(before.get(i).getEmbedding(), after.get(i).getEmbedding());
        }
        assertArrayEquals(original, Files.readAllBytes(file));
        assertTrue(wrapped.getEntityVectors(empty).isEmpty());
        assertEquals(EntityType.EDGE, wrapped.getEntityVectors(edge).get(0).getEntity().getEntityType());
    }

    @Test
    void rejectsMissingMetadataAndInvalidSearchArguments() {
        assertThrows(IllegalArgumentException.class,
            () -> new EntityVectorMetadata("", 2, "i", "v", CREATED_AT));
        assertThrows(IllegalArgumentException.class,
            () -> new EntityVectorMetadata("m", 0, "i", "v", CREATED_AT));
        assertThrows(IllegalArgumentException.class,
            () -> new EntityVectorMetadata("m", 2, "", "v", CREATED_AT));
        assertThrows(IllegalArgumentException.class,
            () -> new EntityVectorMetadata("m", 2, "i", "", CREATED_AT));
        assertThrows(NullPointerException.class,
            () -> new EntityVectorMetadata("m", 2, "i", "v", null));
        assertThrows(IllegalArgumentException.class, () -> index.search(graph,
            query("test-model", vector(1, 0)), 0, 0.5, null));
        assertThrows(IllegalArgumentException.class, () -> index.search(graph,
            query("test-model", vector(1, 0)), 1, Double.NaN, null));
    }

    @Test
    void emptyAndZeroQueriesProduceNoHits() {
        assertTrue(index.search(graph, query("test-model"), 10, 0.5, null).isEmpty());
        assertTrue(index.search(graph, query("test-model", vector(0, 0)), 10, 0.0, null).isEmpty());
        embeddings.clear();
        assertTrue(index.search(graph, query("test-model", vector(1, 0)), 10, 0.0, null).isEmpty());
    }

    @Test
    void rejectsAmbiguousLegacyKeysInsteadOfAssigningVectorsToTheWrongEntity() {
        GraphSchema schema = new GraphSchema();
        Map<String, EntityGroup> groups = new HashMap<>();
        VertexSchema left = new VertexSchema("bc", "id", Collections.emptyList());
        VertexSchema right = new VertexSchema("c", "id", Collections.emptyList());
        schema.addVertex(left);
        schema.addVertex(right);
        groups.put("bc", new VertexGroup(left, Collections.singletonList(
            new Vertex("bc", "a", Collections.emptyList()))));
        groups.put("c", new VertexGroup(right, Collections.singletonList(
            new Vertex("c", "ab", Collections.emptyList()))));
        LocalMemoryGraphAccessor collisionGraph =
            new LocalMemoryGraphAccessor(new MemoryGraph(schema, groups));
        EmbeddingIndexStore legacy = new EmbeddingIndexStore() {
            @Override
            public List<IVector> getEntityIndex(GraphEntity entity) {
                return Collections.singletonList(vector(1, 0));
            }
        };
        EntityVectorIndex wrapped = new EntityVectorIndex(legacy, metadata);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> wrapped.search(collisionGraph, query("test-model", vector(1, 0)), 1, 0.5, null));
        assertTrue(error.getMessage().contains("Ambiguous legacy embedding key"));
    }

    private List<List<GraphEntity>> results(IndexStore store, VectorSearch query) {
        return new EmbeddingOperator(graph, store).apply(null, query).stream()
            .map(SubGraph::getGraphEntityList).collect(Collectors.toList());
    }

    private EmbeddingIndexStore load(Path file) {
        EmbeddingIndexStore store = new EmbeddingIndexStore();
        store.initStore(graph, new VerbalizationFunction() {
            @Override
            public List<String> verbalize(GraphEntity entity) {
                assertEquals(empty, entity, "all nonempty entities must load from the fixture");
                return Collections.emptyList();
            }

            @Override
            public String verbalize(SubGraph subGraph) {
                throw new AssertionError("Unexpected subgraph verbalization");
            }

            @Override
            public String verbalizeGraphSchema() {
                throw new AssertionError("Unexpected schema verbalization");
            }
        }, file.toString(), new ModelConfig("test-model", "http://127.0.0.1:1", "", ""));
        return store;
    }

    private static EmbeddingVector vector(double... values) {
        return new EmbeddingVector(values);
    }

    private static VectorSearch query(String model, EmbeddingVector... vectors) {
        VectorSearch query = new VectorSearch("memory", "session");
        query.setEmbeddingModel(model);
        query.addVector(new ArrayList<IVector>(Arrays.asList(vectors)));
        return query;
    }
}
