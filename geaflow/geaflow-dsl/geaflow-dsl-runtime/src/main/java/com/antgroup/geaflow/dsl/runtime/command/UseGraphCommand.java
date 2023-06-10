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

import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.QueryResult;
import com.antgroup.geaflow.dsl.sqlnode.SqlUseGraph;
import org.apache.calcite.sql.SqlNode;

public class UseGraphCommand implements IQueryCommand {

    private final SqlUseGraph useGraph;

    public UseGraphCommand(SqlUseGraph useGraph) {
        this.useGraph = useGraph;
    }

    @Override
    public QueryResult execute(QueryContext context) {
        context.getGqlContext().setCurrentGraph(useGraph.getGraph());
        return new QueryResult(true);
    }

    @Override
    public SqlNode getSqlNode() {
        return useGraph;
    }
}
