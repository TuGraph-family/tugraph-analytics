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

import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;

public class WhereFunctionImpl implements WhereFunction {

    private final Expression condition;

    public WhereFunctionImpl(Expression condition) {
        this.condition = condition;
    }

    @Override
    public boolean filter(Row row) {
        Boolean accept = (Boolean) condition.evaluate(row);
        return accept != null && accept;
    }

    @Override
    public Expression getCondition() {
        return condition;
    }
}
