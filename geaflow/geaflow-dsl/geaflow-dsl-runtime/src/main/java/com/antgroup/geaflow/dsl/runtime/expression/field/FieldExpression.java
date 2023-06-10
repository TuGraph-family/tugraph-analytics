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
import com.antgroup.geaflow.dsl.runtime.expression.AbstractExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import java.util.Collections;
import java.util.List;

public class FieldExpression extends AbstractExpression {

    private final Expression input;

    protected final int fieldIndex;

    protected final IType<?> outputType;

    public FieldExpression(Expression input, int fieldIndex, IType<?> outputType) {
        this.input = input;
        this.fieldIndex = fieldIndex;
        this.outputType = outputType;
    }

    public FieldExpression(int fieldIndex, IType<?> outputType) {
        this(null, fieldIndex, outputType);
    }

    @Override
    public Object evaluate(Row row) {
        Row inputR = row;
        if (input != null) {
            inputR = (Row) input.evaluate(row);
        }
        if (inputR == null) {
            return null;
        }
        return inputR.getField(fieldIndex, outputType);
    }

    @Override
    public String showExpression() {
        if (input != null) {
            return input.showExpression() + ".$" + fieldIndex;
        }
        return "$" + fieldIndex;
    }

    @Override
    public IType<?> getOutputType() {
        return outputType;
    }

    @Override
    public List<Expression> getInputs() {
        if (input != null) {
            return Collections.singletonList(input);
        }
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        if (input == null) {
            assert inputs.isEmpty();
            return new FieldExpression(fieldIndex, outputType);
        }
        assert inputs.size() == 1;
        return new FieldExpression(inputs.get(0), fieldIndex, outputType);
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public FieldExpression copy(int newIndex) {
        return new FieldExpression(input, newIndex, outputType);
    }
}
