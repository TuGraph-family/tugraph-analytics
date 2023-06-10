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

import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.Objects;

public class ValueTimeEdge<K, EV> extends ValueEdge<K, EV> implements IGraphElementWithTimeField {

    private long time;

    public ValueTimeEdge() {
    }

    public ValueTimeEdge(K src, K target, EV value) {
        this(src, target, value, 0);
    }

    public ValueTimeEdge(K src, K target, EV value, long time) {
        this(src, target, value, EdgeDirection.OUT, time);
    }

    public ValueTimeEdge(K src, K target, EV value, EdgeDirection edgeDirection, long time) {
        super(src, target, value, edgeDirection);
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
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        ValueTimeEdge<?, ?> that = (ValueTimeEdge<?, ?>) o;
        return this.time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.time);
    }

}
