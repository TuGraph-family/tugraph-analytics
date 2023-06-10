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

package com.antgroup.geaflow.dsl.common.data.impl.types;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.types.EdgeType;
import com.antgroup.geaflow.model.graph.IGraphElementWithTimeField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Objects;
import java.util.function.Supplier;

public class ObjectTsEdge extends ObjectEdge implements IGraphElementWithTimeField {

    public static final Supplier<ObjectTsEdge> CONSTRUCTOR = new Constructor();

    private long time;

    public ObjectTsEdge() {

    }

    public ObjectTsEdge(Object srcId, Object targetId) {
        super(srcId, targetId);
    }

    public ObjectTsEdge(Object srcId, Object targetId, Row value) {
        super(srcId, targetId, value);
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

    @Override
    public Object getField(int i, IType<?> type) {
        switch (i) {
            case EdgeType.SRC_ID_FIELD_POSITION:
                return getSrcId();
            case EdgeType.TARGET_ID_FIELD_POSITION:
                return getTargetId();
            case EdgeType.LABEL_FIELD_POSITION:
                return getBinaryLabel();
            case EdgeType.TIME_FIELD_POSITION:
                return time;
            default:
                return getValue().getField(i - 4, type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ObjectTsEdge)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ObjectTsEdge that = (ObjectTsEdge) o;
        return time == that.time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), time);
    }

    @Override
    public ObjectTsEdge reverse() {
        ObjectTsEdge edge = new ObjectTsEdge(getTargetId(), getSrcId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge withDirection(EdgeDirection direction) {
        ObjectTsEdge edge = new ObjectTsEdge(getSrcId(), getTargetId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(direction);
        edge.setTime(getTime());
        return edge;
    }

    @Override
    public ObjectEdge withValue(Row value) {
        ObjectTsEdge edge = new ObjectTsEdge(getSrcId(), getTargetId(), value);
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect());
        edge.setTime(time);
        return edge;
    }

    @Override
    public RowEdge identityReverse() {
        ObjectTsEdge edge = new ObjectTsEdge(getTargetId(), getSrcId(), getValue());
        edge.setBinaryLabel(getBinaryLabel());
        edge.setDirect(getDirect().reverse());
        edge.setTime(time);
        return edge;
    }

    @Override
    public String toString() {
        return getSrcId() + "#" + getTargetId() + "#" + getBinaryLabel() + "#" + getDirect()
            + "#" + time + "#" + getValue();
    }

    private static class Constructor implements Supplier<ObjectTsEdge> {

        @Override
        public ObjectTsEdge get() {
            return new ObjectTsEdge();
        }
    }
}
