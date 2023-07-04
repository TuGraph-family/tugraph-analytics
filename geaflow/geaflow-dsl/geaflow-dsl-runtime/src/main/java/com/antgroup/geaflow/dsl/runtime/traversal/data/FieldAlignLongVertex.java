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

package com.antgroup.geaflow.dsl.runtime.traversal.data;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.types.LongVertex;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import java.util.Objects;

public class FieldAlignLongVertex extends FieldAlignVertex implements RowVertex {

    private final LongVertex baseVertex;

    private final int[] fieldMapping;

    public FieldAlignLongVertex(LongVertex baseVertex, int[] fieldMapping) {
        super(baseVertex, fieldMapping);
        this.baseVertex = baseVertex;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public IVertex<Object, Row> withValue(Row value) {
        return new FieldAlignLongVertex(baseVertex.withValue(value), fieldMapping);
    }

    @Override
    public IVertex<Object, Row> withLabel(String label) {
        return new FieldAlignLongVertex(baseVertex.withLabel(label), fieldMapping);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowVertex)) {
            return false;
        }
        if (o instanceof LongVertex) {
            LongVertex that = (LongVertex) o;
            return baseVertex.id == that.id && Objects.equals(baseVertex.getBinaryLabel(),
                that.getBinaryLabel());
        } else if (o instanceof FieldAlignLongVertex) {
            FieldAlignLongVertex that = (FieldAlignLongVertex) o;
            return baseVertex.id == that.baseVertex.id && Objects.equals(baseVertex.getBinaryLabel(),
                that.getBinaryLabel());
        } else {
            RowVertex that = (RowVertex) o;
            return getId().equals(that.getId()) && Objects.equals(getBinaryLabel(), that.getBinaryLabel());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseVertex.id, getBinaryLabel());
    }
}
