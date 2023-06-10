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

package com.antgroup.geaflow.dsl.runtime.expression.condition;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.google.common.collect.Lists;
import java.util.List;

public class IfExpression extends AbstractNonLeafExpression {

    public IfExpression(Expression condition, Expression trueValue, Expression falseValue, IType<?> outputType) {
        super(Lists.newArrayList(condition, trueValue, falseValue), outputType);
    }

    @Override
    public Object evaluate(Row row) {
        Boolean condition = (Boolean) inputs.get(0).evaluate(row);
        if (condition != null && condition) {
            return inputs.get(1).evaluate(row);
        }
        return inputs.get(2).evaluate(row);
    }

    @Override
    public String showExpression() {
        return "if(" + inputs.get(0).showExpression() + ", " + inputs.get(1).showExpression()
            + ", " + inputs.get(2).showExpression() + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 3;
        return new IfExpression(inputs.get(0), inputs.get(1), inputs.get(2), getOutputType());
    }
}
