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

package com.antgroup.geaflow.dsl.runtime.expression.literal;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.util.BinaryUtil;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.util.StringLiteralUtil;
import java.util.Collections;
import java.util.List;

public class LiteralExpression extends AbstractExpression {

    private final Object value;
    private final String literalString;
    private final IType<?> outputType;

    public LiteralExpression(Object value, IType<?> outputType) {
        this.value = BinaryUtil.toBinaryForString(value);
        this.outputType = outputType;
        if (outputType == Types.STRING) {
            this.literalString = StringLiteralUtil.escapeSQLString(String.valueOf(value));
        } else {
            literalString = String.valueOf(value);
        }
    }

    @Override
    public Object evaluate(Row row) {
        return value;
    }

    @Override
    public String showExpression() {
        return literalString;
    }

    @Override
    public IType<?> getOutputType() {
        return outputType;
    }

    @Override
    public List<Expression> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 0;
        return new LiteralExpression(value, outputType);
    }

    public Object getValue() {
        return value;
    }

    public static LiteralExpression createBoolean(boolean value) {
        return new LiteralExpression(value, Types.BOOLEAN);
    }
}
