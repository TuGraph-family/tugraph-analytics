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

public class NotExpression extends AbstractNonLeafExpression {

    public NotExpression(Expression input) {
        super(Collections.singletonList(input), Types.BOOLEAN);
    }

    @Override
    public Object evaluate(Row row) {
        Boolean condition = (Boolean) inputs.get(0).evaluate(row);
        if (condition == null) {
            return null;
        }
        return !condition;
    }

    @Override
    public String showExpression() {
        return "Not(" + inputs.get(0).showExpression() + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 1;
        return new NotExpression(inputs.get(0));
    }
}
