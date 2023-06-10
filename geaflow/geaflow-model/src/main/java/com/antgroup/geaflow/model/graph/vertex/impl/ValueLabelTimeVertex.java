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

import com.antgroup.geaflow.model.graph.IGraphElementWithLabelField;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;

import java.util.Objects;

public class ValueLabelTimeVertex<K, VV> extends ValueVertex<K, VV>
    implements IGraphElementWithLabelField, IGraphElementWithTimeField {

    private String label;
    private long time;

    public ValueLabelTimeVertex() {
    }

    public ValueLabelTimeVertex(K id) {
        super(id);
    }

    public ValueLabelTimeVertex(K id, VV value, String label, long time) {
        super(id, value);
        this.label = label;
        this.time = time;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
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
        this.label = label;
        return this;
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
        ValueLabelTimeVertex<?, ?> that = (ValueLabelTimeVertex<?, ?>) o;
        return this.time == that.time && Objects.equals(this.label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.label, this.time);
    }

}
