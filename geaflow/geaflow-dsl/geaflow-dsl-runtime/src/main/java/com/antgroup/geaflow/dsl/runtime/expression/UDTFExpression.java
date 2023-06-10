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
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.function.UDTF;
import java.util.List;

public class UDTFExpression extends UDFExpression {

    public UDTFExpression(List<Expression> inputs, IType<?> outputType,
                          Class<?> implementClass) {
        super(inputs, outputType, implementClass);
    }

    @Override
    public Object evaluate(Row row) {
        super.evaluate(row);
        return ((UDTF) implementInstance).getCollectData();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new UDTFExpression(inputs, outputType, implementClass);
    }
}
