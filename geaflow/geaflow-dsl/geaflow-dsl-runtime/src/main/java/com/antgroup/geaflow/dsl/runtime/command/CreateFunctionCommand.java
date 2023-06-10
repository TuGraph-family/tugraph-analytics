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

package com.antgroup.geaflow.dsl.runtime.command;

import com.antgroup.geaflow.dsl.planner.GQLContext;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.QueryResult;
import com.antgroup.geaflow.dsl.schema.GeaFlowFunction;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateFunction;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateFunctionCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateFunctionCommand.class);

    private final SqlCreateFunction createFunction;

    public CreateFunctionCommand(SqlCreateFunction createFunction) {
        this.createFunction = createFunction;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        GQLContext gqlContext = context.getGqlContext();
        GeaFlowFunction function = GeaFlowFunction.toFunction(createFunction);
        // register function to catalog.
        gqlContext.registerFunction(function);
        LOGGER.info("Success to create function: \n{}", function);
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return createFunction;
    }
}
