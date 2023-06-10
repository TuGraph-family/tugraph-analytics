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

package com.antgroup.geaflow.model.graph.edge.impl;

import com.antgroup.geaflow.model.graph.IGraphElementWithLabelField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.Objects;

public class ValueLabelEdge<K, EV> extends ValueEdge<K, EV> implements IGraphElementWithLabelField {

    private String label;

    public ValueLabelEdge() {
    }

    public ValueLabelEdge(K src, K target, EV value) {
        this(src, target, value, null);
    }

    public ValueLabelEdge(K src, K target, EV value, String label) {
        this(src, target, value, EdgeDirection.OUT, label);
    }

    public ValueLabelEdge(K srcId, K targetId, EV value, EdgeDirection edgeDirection, String label) {
        super(srcId, targetId, value, edgeDirection);
        this.label = label;
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
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ValueLabelEdge<?, ?> that = (ValueLabelEdge<?, ?>) o;
        return Objects.equals(this.label, that.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.label);
    }

}
