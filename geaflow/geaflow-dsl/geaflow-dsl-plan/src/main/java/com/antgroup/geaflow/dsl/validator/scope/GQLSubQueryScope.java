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

import com.antgroup.geaflow.dsl.calcite.PathRecordType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class GQLSubQueryScope extends GQLScope {

    public GQLSubQueryScope(SqlValidatorScope parent, SqlNode node) {
        super(parent, node);
    }

    public PathRecordType getInputPathType() {
        assert children.size() == 1 : "GQLSubQueryScope must have only one child namespace.";
        return (PathRecordType) children.get(0).getNamespace().getType();
    }
}
