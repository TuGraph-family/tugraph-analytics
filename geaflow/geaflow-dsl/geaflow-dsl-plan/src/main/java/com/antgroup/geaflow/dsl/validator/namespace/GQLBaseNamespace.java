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

import com.antgroup.geaflow.dsl.validator.GQLValidatorImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.AbstractNamespace;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public abstract class GQLBaseNamespace extends AbstractNamespace  {

    public GQLBaseNamespace(SqlValidatorImpl validator,
                            SqlNode enclosingNode) {
        super(validator, enclosingNode);
    }

    @Override
    public GQLValidatorImpl getValidator() {
        return (GQLValidatorImpl) validator;
    }

    public boolean isCaseSensitive() {
        return ((GQLValidatorImpl) validator).isCaseSensitive();
    }
}
