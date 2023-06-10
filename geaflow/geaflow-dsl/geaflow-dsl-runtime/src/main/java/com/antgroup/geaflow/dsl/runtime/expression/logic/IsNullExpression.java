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

package com.antgroup.geaflow.dsl.runtime.expression.logic;

import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractNonLeafExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import java.util.Collections;
import java.util.List;

public class IsNullExpression extends AbstractNonLeafExpression {

    public IsNullExpression(Expression input) {
        super(Collections.singletonList(input), Types.BOOLEAN);
    }

    @Override
    public Object evaluate(Row row) {
        return inputs.get(0).evaluate(row) == null;
    }

    @Override
    public String showExpression() {
        return inputs.get(0).showExpression() + " is null";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 1;
        return new IsNullExpression(inputs.get(0));
    }
}
