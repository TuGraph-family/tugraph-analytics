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

package com.antgroup.geaflow.dsl.common.data.impl;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import java.util.Collection;
import java.util.List;

public class DefaultParameterizedPath implements ParameterizedPath {

    private final Path basePath;

    private final Object requestId;

    private final Row parameterRow;

    private final Row systemVariableRow;

    public DefaultParameterizedPath(Path basePath, Object requestId, Row parameterRow,
                                    Row systemVariableRow) {
        this.basePath = basePath;
        this.requestId = requestId;
        this.parameterRow = parameterRow;
        this.systemVariableRow = systemVariableRow;
    }

    public DefaultParameterizedPath(Path basePath, Object requestId, Row parameterRow) {
        this(basePath, requestId, parameterRow, null);
    }

    @Override
    public Row getParameter() {
        return parameterRow;
    }

    @Override
    public Row getSystemVariables() {
        return systemVariableRow;
    }

    @Override
    public Object getRequestId() {
        return requestId;
    }

    @Override
    public Row getField(int i, IType<?> type) {
        return basePath.getField(i, type);
    }

    @Override
    public List<Row> getPathNodes() {
        return basePath.getPathNodes();
    }

    @Override
    public void addNode(Row node) {
        basePath.addNode(node);
    }

    @Override
    public void remove(int index) {
        basePath.remove(index);
    }

    @Override
    public Path copy() {
        return new DefaultParameterizedPath(basePath.copy(), requestId,
            parameterRow, systemVariableRow);
    }

    @Override
    public int size() {
        return basePath.size();
    }

    @Override
    public Path subPath(Collection<Integer> indices) {
        return new DefaultParameterizedPath(basePath.subPath(indices), requestId,
            parameterRow, systemVariableRow);
    }

    @Override
    public Path subPath(int[] indices) {
        return new DefaultParameterizedPath(basePath.subPath(indices), requestId,
            parameterRow, systemVariableRow);
    }
}
