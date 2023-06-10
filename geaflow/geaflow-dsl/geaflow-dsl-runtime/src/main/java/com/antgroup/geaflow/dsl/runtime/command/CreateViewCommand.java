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
import com.antgroup.geaflow.dsl.schema.GeaFlowView;
import com.antgroup.geaflow.dsl.sqlnode.SqlCreateView;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateViewCommand implements IQueryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateViewCommand.class);

    private final SqlCreateView createView;

    public CreateViewCommand(SqlCreateView createView) {
        this.createView = createView;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        GQLContext gqlContext = context.getGqlContext();
        GeaFlowView view = gqlContext.convertToView(createView);
        // register view to catalog.
        gqlContext.registerView(view);
        LOGGER.info("Success to create view: \n{}", view);

        IQueryCommand command = context.getCommand(createView.getSubQuery());
        boolean preIsCompile = context.setCompile(true);
        QueryResult viewResult = command.execute(context);
        context.putViewDataView(view.getName(), viewResult.getDataView());
        context.setCompile(preIsCompile);
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return createView;
    }
}
