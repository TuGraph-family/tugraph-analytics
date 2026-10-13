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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geaflow.ai.common.model.ModelUtils;
import org.apache.geaflow.ai.graph.GraphAccessor;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.index.EmbeddingIndexStore;
import org.apache.geaflow.ai.index.IndexStore;
import org.apache.geaflow.ai.index.vector.EmbeddingVector;
import org.apache.geaflow.ai.index.vector.IVector;
import org.apache.geaflow.ai.index.vector.VectorType;
import org.apache.geaflow.ai.search.VectorSearch;

/**
 * Entity semantics and provenance validation over the existing embedding store.
 *
 * <p>The unbound constructor provides legacy lookup compatibility only. Typed
 * lookup and search require an explicit metadata binding for the stored vectors.
 * This wrapper neither builds embeddings nor changes the legacy JSONL format.
 * Like the underlying store, it requires external synchronization during writes.
 */
public final class EntityVectorIndex implements IndexStore {

    private final IndexStore delegate;
    private final EntityVectorMetadata metadata;

    public EntityVectorIndex(IndexStore delegate) {
        this(delegate, null);
    }

    public EntityVectorIndex(IndexStore delegate, EntityVectorMetadata metadata) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.metadata = metadata;
    }

    @Override
    public List<IVector> getEntityIndex(GraphEntity entity) {
        List<IVector> vectors = delegate.getEntityIndex(entity);
        if (metadata != null) {
            for (IVector vector : vectors) {
                metadata.validateVector(embedding(vector).getValues());
            }
        }
        return vectors;
    }

    public List<EntityVectorRecord> getEntityVectors(GraphEntity entity) {
        requireMetadata();
        if (entity == null) {
            return Collections.emptyList();
        }
        EntityReference reference = EntityReference.of(entity);
        List<IVector> vectors = getEntityIndex(entity);
        List<EntityVectorRecord> records = new ArrayList<>(vectors.size());
        for (int i = 0; i < vectors.size(); i++) {
            records.add(new EntityVectorRecord(reference, i,
                embedding(vectors.get(i)).getValues(), metadata));
        }
        return Collections.unmodifiableList(records);
    }

    /**
     * Validates a bound index before the legacy operator performs any scoring.
     * Unbound wrappers retain the original operator behavior.
     */
    public void validateSearch(GraphAccessor graph, String queryModel, List<IVector> queries) {
        if (metadata == null) {
            return;
        }
        validateQueries(queryModel, queries);
        collectRecords(graph);
    }

    /**
     * Linear entity lookup, optionally restricted to a vertex or edge kind.
     *
     * <p>As in EmbeddingOperator, each entity is scored by the maximum cosine
     * across query and entity segments, starting at zero, with a strict threshold.
     * This new API breaks equal-score ties by entity ID; the legacy operator's
     * candidate selection and tie behavior are untouched. A null kind includes both.
     */
    public List<EntityVectorHit> search(GraphAccessor graph, VectorSearch query,
                                       int topK, double threshold, EntityType kind) {
        requireMetadata();
        Objects.requireNonNull(query, "query");
        if (topK <= 0) {
            throw new IllegalArgumentException("topK must be positive");
        }
        if (!Double.isFinite(threshold) || threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("threshold must be between zero and one");
        }
        List<IVector> queries = query.getVectorMap().get(VectorType.EmbeddingVector);
        if (queries == null) {
            queries = Collections.emptyList();
        }
        List<EmbeddingVector> queryVectors = validateQueries(query.getEmbeddingModel(), queries);
        // Validate the complete index before scoring, including entries beyond topK.
        Map<EntityReference, List<EntityVectorRecord>> records = collectRecords(graph);
        if (queryVectors.isEmpty()) {
            return Collections.emptyList();
        }
        List<EntityVectorHit> hits = new ArrayList<>();
        for (Map.Entry<EntityReference, List<EntityVectorRecord>> entry : records.entrySet()) {
            if (kind != null && entry.getKey().getEntityType() != kind) {
                continue;
            }
            double score = 0.0;
            EntityVectorRecord best = null;
            for (EntityVectorRecord record : entry.getValue()) {
                EmbeddingVector candidate = new EmbeddingVector(record.getEmbedding());
                for (EmbeddingVector queryVector : queryVectors) {
                    double match = queryVector.match(candidate);
                    if (match > score) {
                        score = match;
                        best = record;
                    }
                }
            }
            if (best != null && score > threshold) {
                hits.add(new EntityVectorHit(best, score));
            }
        }
        hits.sort(Comparator.comparingDouble(EntityVectorHit::getScore).reversed()
            .thenComparing(hit -> hit.getEntity().getEntityId()));
        return new ArrayList<>(hits.subList(0, Math.min(topK, hits.size())));
    }

    private List<EmbeddingVector> validateQueries(String queryModel, List<IVector> queries) {
        requireMetadata();
        metadata.validateModel(queryModel);
        Objects.requireNonNull(queries, "queries");
        List<EmbeddingVector> result = new ArrayList<>(queries.size());
        for (IVector vector : queries) {
            double[] values = embedding(vector).getValues();
            metadata.validateVector(values);
            result.add(new EmbeddingVector(values));
        }
        return result;
    }

    private Map<EntityReference, List<EntityVectorRecord>> collectRecords(GraphAccessor graph) {
        Objects.requireNonNull(graph, "graph");
        Map<EntityReference, List<EntityVectorRecord>> records = new LinkedHashMap<>();
        Map<String, EntityReference> legacyKeys = new HashMap<>();
        Iterator<GraphVertex> vertices = graph.scanVertex();
        while (vertices.hasNext()) {
            GraphVertex vertex = vertices.next();
            collectEntity(vertex, records, legacyKeys);
            Iterator<GraphEdge> edges = graph.scanEdge(vertex);
            while (edges.hasNext()) {
                collectEntity(edges.next(), records, legacyKeys);
            }
        }
        return records;
    }

    private void collectEntity(GraphEntity entity,
                               Map<EntityReference, List<EntityVectorRecord>> records,
                               Map<String, EntityReference> legacyKeys) {
        EntityReference reference = EntityReference.of(entity);
        if (delegate instanceof EmbeddingIndexStore) {
            String key = ModelUtils.getGraphEntityKey(entity);
            EntityReference previous = legacyKeys.putIfAbsent(key, reference);
            if (previous != null && !previous.equals(reference)) {
                throw new IllegalArgumentException("Ambiguous legacy embedding key: " + key);
            }
        }
        if (!records.containsKey(reference)) {
            records.put(reference, getEntityVectors(entity));
        }
    }

    private static EmbeddingVector embedding(IVector vector) {
        if (!(vector instanceof EmbeddingVector)) {
            throw new IllegalArgumentException("Entity vector index requires EmbeddingVector values");
        }
        return (EmbeddingVector) vector;
    }

    private void requireMetadata() {
        if (metadata == null) {
            throw new IllegalStateException("Entity vector metadata is missing; bind known build "
                + "provenance before typed lookup or search");
        }
    }
}
