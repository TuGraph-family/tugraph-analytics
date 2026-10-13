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

import java.util.Objects;
import org.apache.geaflow.ai.graph.GraphEdge;
import org.apache.geaflow.ai.graph.GraphEntity;
import org.apache.geaflow.ai.graph.GraphVertex;
import org.apache.geaflow.ai.graph.io.Edge;
import org.apache.geaflow.ai.graph.io.Vertex;

/**
 * Stable identity following the current graph equality contract.
 *
 * <p>Vertices use (label, id); directed edges use (label, source, destination).
 * Length-prefixed components avoid concatenation collisions. Parallel edges with
 * identical endpoints and label have the same identity, as in the current graph model.
 */
public final class EntityReference {

    private final EntityType entityType;
    private final String entityId;
    private final String label;

    private EntityReference(EntityType entityType, String label, String... components) {
        this.entityType = entityType;
        this.label = Objects.requireNonNull(label, "label");
        StringBuilder id = new StringBuilder("entity:v1:").append(entityType).append(':');
        append(id, label);
        for (String component : components) {
            append(id, component);
        }
        this.entityId = id.toString();
    }

    public static EntityReference of(GraphEntity entity) {
        Objects.requireNonNull(entity, "entity");
        if (entity instanceof GraphVertex) {
            Vertex vertex = ((GraphVertex) entity).getVertex();
            return new EntityReference(EntityType.VERTEX, vertex.getLabel(), vertex.getId());
        }
        if (entity instanceof GraphEdge) {
            Edge edge = ((GraphEdge) entity).getEdge();
            return new EntityReference(EntityType.EDGE, edge.getLabel(),
                edge.getSrcId(), edge.getDstId());
        }
        throw new IllegalArgumentException("Unsupported graph entity: " + entity.getClass().getName());
    }

    private static void append(StringBuilder builder, String value) {
        Objects.requireNonNull(value, "identity component");
        builder.append(value.length()).append(':').append(value);
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EntityReference
            && entityId.equals(((EntityReference) other).entityId);
    }

    @Override
    public int hashCode() {
        return entityId.hashCode();
    }
}
