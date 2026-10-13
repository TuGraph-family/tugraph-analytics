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

import java.time.Instant;
import java.util.Objects;

/**
 * Explicit provenance binding for an existing embedding index.
 *
 * <p>Legacy JSONL has no model identity. The caller must supply provenance from
 * the index build, never infer it from the current query. This entity-local binding
 * can be adapted to the VectorStore metadata contract when issue #851 lands.
 */
public final class EntityVectorMetadata {

    private final String modelName;
    private final int dimension;
    private final String indexVersion;
    private final String vectorVersion;
    private final Instant createdAt;

    public EntityVectorMetadata(String modelName, int dimension, String indexVersion,
                                String vectorVersion, Instant createdAt) {
        this.modelName = required(modelName, "modelName");
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive");
        }
        this.dimension = dimension;
        this.indexVersion = required(indexVersion, "indexVersion");
        this.vectorVersion = required(vectorVersion, "vectorVersion");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
    }

    private static String required(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    public String getModelName() {
        return modelName;
    }

    public int getDimension() {
        return dimension;
    }

    public String getDistance() {
        return "cosine";
    }

    public String getIndexVersion() {
        return indexVersion;
    }

    public String getVectorVersion() {
        return vectorVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public int getFormatVersion() {
        return 1;
    }

    void validateModel(String queryModel) {
        required(queryModel, "query model");
        if (!modelName.equals(queryModel)) {
            throw new IllegalArgumentException("Embedding model mismatch: expected "
                + modelName + ", got " + queryModel);
        }
    }

    void validateVector(double[] values) {
        Objects.requireNonNull(values, "embedding");
        if (values.length != dimension) {
            throw new IllegalArgumentException("Embedding dimension mismatch: expected "
                + dimension + ", got " + values.length);
        }
        for (double value : values) {
            if (!Double.isFinite(value)) {
                throw new IllegalArgumentException("Embedding values must be finite");
            }
        }
    }
}
