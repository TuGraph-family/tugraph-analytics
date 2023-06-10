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

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.IEdge;

import java.util.Objects;

public class IDEdge<K> implements IEdge<K, Object> {

    private K srcId;
    private K targetId;
    private EdgeDirection direction;

    public IDEdge() {
    }

    public IDEdge(K srcId, K targetId) {
        this(srcId, targetId, EdgeDirection.OUT);
    }

    public IDEdge(K srcId, K targetId, EdgeDirection edgeDirection) {
        this.srcId = srcId;
        this.targetId = targetId;
        this.direction = edgeDirection;
    }

    @Override
    public K getSrcId() {
        return this.srcId;
    }

    @Override
    public void setSrcId(K srcId) {
        this.srcId = srcId;
    }

    @Override
    public K getTargetId() {
        return this.targetId;
    }

    @Override
    public void setTargetId(K targetId) {
        this.targetId = targetId;
    }

    @Override
    public IEdge<K, Object> reverse() {
        return new IDEdge<>(this.targetId, this.srcId);
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public IEdge<K, Object> withValue(Object value) {
        throw new GeaflowRuntimeException(RuntimeErrors.INST.unsupportedError());
    }

    @Override
    public EdgeDirection getDirect() {
        return this.direction;
    }

    @Override
    public void setDirect(EdgeDirection direction) {
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        IDEdge<?> that = (IDEdge<?>) o;
        return Objects.equals(this.srcId, that.srcId)
            && Objects.equals(this.targetId, that.targetId)
            && this.direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.srcId, this.targetId, this.direction);
    }

}
