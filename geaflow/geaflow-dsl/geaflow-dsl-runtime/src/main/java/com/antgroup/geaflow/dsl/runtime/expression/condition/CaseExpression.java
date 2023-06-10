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
import com.google.common.base.Preconditions;
import java.util.List;

public class CaseExpression extends AbstractNonLeafExpression {

    public CaseExpression(List<Expression> inputs, IType<?> outputType) {
        super(inputs, outputType);
        Preconditions.checkArgument(inputs.size() % 2 == 1);
    }

    @Override
    public Object evaluate(Row row) {
        int i = 0;
        while (i < inputs.size() - 1) {
            if (i % 2 == 0) {
                Boolean condition = (Boolean) inputs.get(i).evaluate(row);
                if (condition != null && condition) {
                    return inputs.get(i + 1).evaluate(row);
                }
            }
            i++;
        }
        return inputs.get(i).evaluate(row);
    }

    @Override
    public String showExpression() {
        StringBuilder str = new StringBuilder();
        str.append("case ");
        int i = 0;
        while (i < inputs.size() - 1) {
            str.append(" when ").append(inputs.get(i).showExpression())
                .append(" then ").append(inputs.get(i + 1).showExpression());
            i += 2;
        }
        str.append(" else ").append(inputs.get(i).showExpression()).append(" end");
        return str.toString();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new CaseExpression(inputs, getOutputType());
    }
}
