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

package com.antgroup.geaflow.model.graph;

import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.io.Serializable;

public class GraphRecord<V, E> implements Serializable {

    private Object record;

    public GraphRecord() {

    }

    public GraphRecord(Object e) {
        this.record = e;
    }

    public ViewType getViewType() {
        if (record instanceof IVertex) {
            return ViewType.vertex;
        } else {
            return ViewType.edge;
        }
    }

    public V getVertex() {
        return (V) record;
    }

    public E getEdge() {
        return (E) record;
    }

    @Override
    public String toString() {
        return record.toString();
    }

    public enum ViewType {
        vertex, edge
    }
}
