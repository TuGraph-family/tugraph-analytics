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

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;

import java.util.Objects;

public class ValueTimeVertex<K, VV> extends ValueVertex<K, VV> implements IGraphElementWithTimeField {

    private long time;

    public ValueTimeVertex() {
    }

    public ValueTimeVertex(K id) {
        super(id);
    }

    public ValueTimeVertex(K id, VV value, long time) {
        super(id, value);
        this.time = time;
    }

    @Override
    public long getTime() {
        return this.time;
    }

    @Override
    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public ValueVertex<K, VV> withLabel(String label) {
        return new ValueLabelTimeVertex<>(this.getId(), this.getValue(), label, this.time);
    }

    @Override
    public ValueVertex<K, VV> withTime(long time) {
        this.time = time;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ValueTimeVertex<?, ?> that = (ValueTimeVertex<?, ?>) o;
        return this.time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.time);
    }

}
