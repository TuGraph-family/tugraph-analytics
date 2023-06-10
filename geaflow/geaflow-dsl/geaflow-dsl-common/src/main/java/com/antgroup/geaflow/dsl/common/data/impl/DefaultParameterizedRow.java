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
import com.antgroup.geaflow.dsl.common.data.ParameterizedRow;
import com.antgroup.geaflow.dsl.common.data.Row;

public class DefaultParameterizedRow implements ParameterizedRow {

    private final Row baseRow;

    private final Object requestId;

    private final Row parameterRow;

    private final Row systemVariableRow;

    public DefaultParameterizedRow(Row baseRow, Object requestId, Row parameterRow, Row systemVariableRow) {
        this.baseRow = baseRow;
        this.requestId = requestId;
        this.parameterRow = parameterRow;
        this.systemVariableRow = systemVariableRow;
    }

    public DefaultParameterizedRow(Row baseRow, Object requestId, Row parameterRow) {
        this(baseRow, requestId, parameterRow, null);
    }

    @Override
    public Object getField(int i, IType<?> type) {
        return baseRow.getField(i, type);
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
}
