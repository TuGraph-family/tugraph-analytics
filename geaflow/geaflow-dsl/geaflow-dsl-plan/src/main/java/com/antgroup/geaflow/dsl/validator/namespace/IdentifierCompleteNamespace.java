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

import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.IdentifierNamespace;

public class IdentifierCompleteNamespace extends IdentifierNamespace {

    private final IdentifierNamespace baseNamespace;
    private final GQLContext gContext;

    public IdentifierCompleteNamespace(IdentifierNamespace baseNamespace) {
        super((GQLValidatorImpl) baseNamespace.getValidator(), baseNamespace.getId(),
            baseNamespace.extendList, baseNamespace.getEnclosingNode(),
            baseNamespace.getParentScope());
        this.baseNamespace = baseNamespace;
        this.gContext = ((GQLValidatorImpl) getValidator()).getGQLContext();
    }

    @Override
    protected SqlIdentifier getResolveId(SqlIdentifier id) {
        return gContext.completeCatalogObjName(id);
    }

    @Override
    public SqlNode getNode() {
        return baseNamespace.getNode();
    }
}
