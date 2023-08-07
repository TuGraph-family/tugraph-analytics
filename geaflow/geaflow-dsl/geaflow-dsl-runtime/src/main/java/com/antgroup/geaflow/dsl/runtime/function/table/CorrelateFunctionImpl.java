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

package com.antgroup.geaflow.dsl.runtime.function.table;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.FunctionContext;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.UDTFExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CorrelateFunctionImpl implements CorrelateFunction {

    private final UDTFExpression expression;

    private final Expression filterExpression;

    private final List<IType<?>> leftOutputTypes;

    private final List<IType<?>> rightOutputTypes;

    public CorrelateFunctionImpl(UDTFExpression expression,
                                 Expression filterExpression,
                                 List<IType<?>> leftOutputTypes,
                                 List<IType<?>> rightOutputTypes) {
        this.expression = Objects.requireNonNull(expression,
            "CorrelateFunctionImpl: expression is null");
        this.filterExpression = filterExpression;
        this.leftOutputTypes = Objects.requireNonNull(leftOutputTypes,
            "CorrelateFunctionImpl: output type is null");
        this.rightOutputTypes = Objects.requireNonNull(rightOutputTypes,
            "CorrelateFunctionImpl: output type is null");
    }

    @Override
    public void open(FunctionContext context) {
        expression.open(context);
    }

    @Override
    public List<Row> process(Row row) {
        List<Row> results = new ArrayList<>();
        for (Object[] value : (List<Object[]>) expression.evaluate(row)) {
            Row newRow = ObjectRow.create(value);
            if (filterExpression == null) {
                results.add(newRow);
            } else {
                Object accept = filterExpression.evaluate(newRow);
                if (accept != null && ((Boolean) accept)) {
                    results.add(newRow);
                }
            }
        }
        return results;
    }

    @Override
    public List<IType<?>> getLeftOutputTypes() {
        return leftOutputTypes;
    }

    @Override
    public List<IType<?>> getRightOutputTypes() {
        return rightOutputTypes;
    }
}
