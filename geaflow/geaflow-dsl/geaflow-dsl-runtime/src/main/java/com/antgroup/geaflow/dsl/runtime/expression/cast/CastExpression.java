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

package com.antgroup.geaflow.dsl.runtime.expression.cast;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.types.ClassType;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractReflectCallExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.literal.LiteralExpression;
import com.antgroup.geaflow.dsl.schema.function.GeaFlowBuiltinFunctions;
import com.google.common.collect.Lists;
import java.util.List;

public class CastExpression extends AbstractReflectCallExpression {

    public static final String METHOD_NAME = "cast";

    public CastExpression(Expression input, IType<?> outputType) {
        super(Lists.newArrayList(input,
                new LiteralExpression(outputType.getTypeClass(), ClassType.INSTANCE)),
            outputType, GeaFlowBuiltinFunctions.class, METHOD_NAME);
    }

    @Override
    public String showExpression() {
        return "cast(" + inputs.get(0).showExpression() + " as " + getOutputType().getTypeClass().getSimpleName() + ")";
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.size() == 2;
        assert inputs.get(1) instanceof LiteralExpression;
        LiteralExpression castTypeLiteral = (LiteralExpression) inputs.get(1);
        assert castTypeLiteral.getValue() == outputType.getTypeClass();
        return new CastExpression(inputs.get(0), outputType);
    }
}
