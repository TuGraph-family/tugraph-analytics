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
import com.antgroup.geaflow.dsl.schema.GeaFlowTable;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateTable;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTableCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableCommand.class);

    private final SqlCreateTable createTable;

    public CreateTableCommand(SqlCreateTable createTable) {
        this.createTable = createTable;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        GQLContext gqlContext = context.getGqlContext();
        GeaFlowTable table = gqlContext.convertToTable(createTable);
        // register table to catalog.
        gqlContext.registerTable(table);
        LOGGER.info("Success to create table: \n{}", table);
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return createTable;
    }
}
