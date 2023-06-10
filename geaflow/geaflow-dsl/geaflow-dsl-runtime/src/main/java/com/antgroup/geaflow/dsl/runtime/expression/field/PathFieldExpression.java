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
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import java.util.List;

public class PathFieldExpression extends FieldExpression {

    private final String label;

    public PathFieldExpression(String label, int fieldIndex, IType<?> outputType) {
        super(fieldIndex, outputType);
        this.label = label;
    }

    @Override
    public Object evaluate(Row row) {
        return row.getField(fieldIndex, outputType);
    }

    @Override
    public String showExpression() {
        return label;
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 0;
        return new PathFieldExpression(label, fieldIndex, outputType);
    }

    @Override
    public PathFieldExpression copy(int newIndex) {
        return new PathFieldExpression(label, newIndex, outputType);
    }
}
