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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

/**
 * Parse tree node that represents a Sql table property.
 */
public class SqlTableProperty extends SqlCall {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("Table Property", SqlKind.OTHER);

    private SqlIdentifier key;
    private SqlNode value;

    public SqlTableProperty(SqlIdentifier key,
                            SqlNode value,
                            SqlParserPos pos) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    public SqlIdentifier getKey() {
        return key;
    }

    public void setKey(SqlIdentifier key) {
        this.key = key;
    }

    public SqlNode getValue() {
        return value;
    }

    public void setValue(SqlNode value) {
        this.value = value;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(getKey(), getValue());
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.key = (SqlIdentifier) operand;
                break;
            case 1:
                this.value = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {
        key.unparse(writer, 0, 0);
        writer.print("=");
        if (value instanceof SqlCharStringLiteral) {
            NlsString nlsString = (NlsString) ((SqlCharStringLiteral) value).getValue();
            writer.print("'" + nlsString.getValue() + "'");
        } else {
            value.unparse(writer, 0, 0);
        }
    }
}
