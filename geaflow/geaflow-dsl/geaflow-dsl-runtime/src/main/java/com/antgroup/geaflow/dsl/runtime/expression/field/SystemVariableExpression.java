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

package com.antgroup.geaflow.dsl.runtime.expression.field;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.ParameterizedRow;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.rex.RexSystemVariable.SystemVariable;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SystemVariableExpression extends AbstractExpression {

    private final SystemVariable variable;

    public SystemVariableExpression(SystemVariable variable) {
        this.variable = Objects.requireNonNull(variable);
    }

    @Override
    public Object evaluate(Row row) {
        ParameterizedRow parameterizedRow = (ParameterizedRow) row;
        return parameterizedRow.getSystemVariables().getField(variable.getIndex(), getOutputType());
    }

    @Override
    public String showExpression() {
        return variable.getName();
    }

    @Override
    public IType<?> getOutputType() {
        return SqlTypeUtil.ofTypeName(variable.getTypeName());
    }

    @Override
    public List<Expression> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 0;
        return new SystemVariableExpression(variable);
    }
}
