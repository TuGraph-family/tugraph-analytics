/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.model.graph.vertex.impl;

import com.antgroup.geaflow.model.graph.vertex.IVertex;

import java.util.Objects;

public class IDVertex<K> implements IVertex<K, Object> {

    private K id;

    public IDVertex() {
    }

    public IDVertex(K id) {
        this.id = id;
    }

    @Override
    public K getId() {
        return this.id;
    }

    @Override
    public void setId(K id) {
        this.id = id;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public ValueVertex<K, Object> withValue(Object value) {
        return new ValueVertex<>(this.id, value);
    }

    @Override
    public IDVertex<K> withLabel(String label) {
        return new IDLabelVertex<>(this.id, label);
    }

    @Override
    public IDVertex<K> withTime(long time) {
        return new IDTimeVertex<>(this.id, time);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IDVertex<?> idVertex = (IDVertex<?>) o;
        return Objects.equals(this.id, idVertex.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    @Override
    public String toString() {
        return String.format("IDVertex(vertexId:%s)", id );
    }

    @Override
    public int compareTo(Object o) {
        IDVertex<K> vertex = (IDVertex<K>) o;
        if (id instanceof Comparable) {
            return ((Comparable<K>) id).compareTo(vertex.getId());
        } else {
            return ((Integer) hashCode()).compareTo(vertex.hashCode());
        }
    }

}
