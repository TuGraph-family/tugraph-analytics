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

package com.antgroup.geaflow.dsl.validator.namespace;

import static org.apache.calcite.util.Static.RESOURCE;

import com.antgroup.geaflow.dsl.sqlnode.SqlFilterStatement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class GQLFilterNamespace extends GQLBaseNamespace {

    private final SqlFilterStatement filterStatement;

    public GQLFilterNamespace(SqlValidatorImpl validator, SqlNode enclosingNode,
                              SqlFilterStatement filterStatement) {
        super(validator, enclosingNode);
        this.filterStatement = filterStatement;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        SqlValidatorNamespace fromNs = validator.getNamespace(filterStatement.getFrom());
        // Validate parent.
        fromNs.validate(targetRowType);

        SqlValidatorScope scope = getValidator().getScopes(filterStatement);

        SqlNode condition = filterStatement.getCondition();
        // expand the condition, e.g. expand the "where id > 10" to "where g0.a.id > 10".
        condition = validator.expand(condition, scope);
        filterStatement.setCondition(condition);

        RelDataType boolType = validator.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
        getValidator().inferUnknownTypes(boolType, scope, condition);
        condition.validate(validator, scope);

        RelDataType conditionType = validator.deriveType(scope, condition);
        if (!SqlTypeUtil.inBooleanFamily(conditionType)) {
            throw validator.newValidationError(condition, RESOURCE.condMustBeBoolean("Filter"));
        }
        // Filter return parent type.
        return fromNs.getType();
    }

    @Override
    public SqlNode getNode() {
        return filterStatement;
    }
}
