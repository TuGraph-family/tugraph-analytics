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

package com.antgroup.geaflow.dsl.runtime.expression;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.function.FunctionContext;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class AbstractNonLeafExpression extends AbstractExpression {

    protected final List<Expression> inputs;

    protected final List<Class<?>> inputTypes;

    protected final IType<?> outputType;

    public AbstractNonLeafExpression(List<Expression> inputs, IType<?> outputType) {
        this.inputs = Objects.requireNonNull(inputs);
        this.inputTypes = inputs.stream()
            .map(Expression::getOutputType)
            .map(IType::getTypeClass)
            .collect(Collectors.toList());
        this.outputType = outputType;
    }

    @Override
    public void open(FunctionContext context) {
        for (Expression input : inputs) {
            input.open(context);
        }
    }

    @Override
    public List<Expression> getInputs() {
        return inputs;
    }

    @Override
    public IType<?> getOutputType() {
        return outputType;
    }
}
