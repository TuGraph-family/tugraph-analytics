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

package com.antgroup.geaflow.dsl.runtime.expression.item;

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.google.common.collect.Lists;
import java.util.List;

public class ItemExpression extends AbstractNonLeafExpression {

    public ItemExpression(Expression target, Expression index) {
        super(Lists.newArrayList(target, index), ((ArrayType) target.getOutputType()).getComponentType());
    }

    @Override
    public Object evaluate(Row row) {
        Object target = inputs.get(0).evaluate(row);
        if (target == null) {
            return null;
        }
        if (target instanceof Object[]) {
            int index = (int) inputs.get(1).evaluate(row);
            return ((Object[]) target)[index];
        }
        throw new IllegalArgumentException("target is not a array object");
    }

    @Override
    public String showExpression() {
        return inputs.get(0).showExpression() + "[" + inputs.get(1).showExpression() + "]";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 2;
        return new ItemExpression(inputs.get(0), inputs.get(1));
    }
}
