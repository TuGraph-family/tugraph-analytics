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

package com.antgroup.geaflow.dsl.planner;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

/**
 * An operator table for look up SQL operators and functions.
 */
public class GQLOperatorTable extends ChainedSqlOperatorTable {

    /**
     * Add a {@code SqlFunction} to operator table.
     */
    public void registerSqlFunction(SqlFunction function) {
        for (SqlOperatorTable operatorTable : tableList) {
            if (operatorTable instanceof ListSqlOperatorTable) {
                ((ListSqlOperatorTable) operatorTable).add(function);
                return;
            }
        }
    }

    public SqlFunction getSqlFunction(String name) {
        for (SqlOperator operator : getOperatorList()) {
            if (operator.getName().equalsIgnoreCase(name)) {
                if (operator instanceof SqlFunction) {
                    return (SqlFunction) operator;
                }
            }
        }
        return null;
    }

    public GQLOperatorTable(SqlOperatorTable... tables) {
        super(Lists.newArrayList(tables));
    }

}
