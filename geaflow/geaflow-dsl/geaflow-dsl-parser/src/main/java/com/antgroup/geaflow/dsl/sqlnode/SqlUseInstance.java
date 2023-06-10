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

package com.antgroup.geaflow.dsl.sqlnode;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlUseInstance extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SqlUseInstance",
        SqlKind.USE_INSTANCE);

    private SqlIdentifier instance;

    public SqlUseInstance(SqlParserPos pos, SqlIdentifier instance) {
        super(pos);
        this.instance = Objects.requireNonNull(instance);
    }

    @Override
    protected void unparseAlterOperation(SqlWriter sqlWriter, int leftPrec, int rightPrec) {
        sqlWriter.keyword("USE");
        sqlWriter.keyword("INSTANCE");
        instance.unparse(sqlWriter, 0, 0);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(instance);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        if (i == 0) {
            this.instance = (SqlIdentifier) operand;
        } else {
            throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    public SqlIdentifier getInstance() {
        return instance;
    }
}
