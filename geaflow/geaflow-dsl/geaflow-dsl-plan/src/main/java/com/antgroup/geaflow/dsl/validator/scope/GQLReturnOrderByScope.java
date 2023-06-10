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

package com.antgroup.geaflow.dsl.validator.scope;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class GQLReturnOrderByScope extends GQLScope {

    private final SqlNodeList orderBy;

    public GQLReturnOrderByScope(SqlValidatorScope parent, SqlNodeList orderBy) {
        super(parent, orderBy);
        this.orderBy = orderBy;
    }

    @Override
    public void validateExpr(SqlNode expr) {
        parent.validateExpr(expr);
    }
}
