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

import java.util.Arrays;
import java.util.Objects;

/** Immutable view of one embedding belonging to a graph entity. */
public final class EntityVectorRecord {

    private final EntityReference entity;
    private final int ordinal;
    private final double[] embedding;
    private final EntityVectorMetadata metadata;

    EntityVectorRecord(EntityReference entity, int ordinal, double[] embedding,
                       EntityVectorMetadata metadata) {
        this.entity = Objects.requireNonNull(entity, "entity");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        metadata.validateVector(embedding);
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must not be negative");
        }
        this.ordinal = ordinal;
        this.embedding = Arrays.copyOf(embedding, embedding.length);
    }

    public EntityReference getEntity() {
        return entity;
    }

    /**
     * The ordinal identifies a segment within a fixed vector version. Rebuilding
     * with different segmentation requires a new vector version.
     */
    public String getVectorId() {
        String version = metadata.getVectorVersion();
        return entity.getEntityId() + ":vector:" + version.length() + ":" + version + ":" + ordinal;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public double[] getEmbedding() {
        return Arrays.copyOf(embedding, embedding.length);
    }

    public EntityVectorMetadata getMetadata() {
        return metadata;
    }
}
