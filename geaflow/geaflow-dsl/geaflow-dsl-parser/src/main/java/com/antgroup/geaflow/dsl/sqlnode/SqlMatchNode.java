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

import com.antgroup.geaflow.dsl.operator.SqlMatchNodeOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

public class SqlMatchNode extends SqlCall {

    private SqlIdentifier name;

    private SqlNodeList labels;

    private SqlNode where;

    public SqlMatchNode(SqlParserPos pos, SqlIdentifier name, SqlNodeList labels, SqlNode where) {
        super(pos);
        this.name = name;
        this.labels = labels;
        this.where  = where;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlMatchNodeOperator.INSTANCE;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.GQL_MATCH_NODE;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, labels, where);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.labels = (SqlNodeList) operand;
                break;
            case 2:
                this.where = operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print("(");
        unparseNode(writer);
        writer.print(")");
    }

    protected void unparseNode(SqlWriter writer) {
        if (name != null) {
            name.unparse(writer, 0, 0);
        }
        if (labels != null && labels.size() > 0) {
            writer.print(":");
            for (int i = 0; i < labels.size(); i++) {
                SqlNode label = labels.get(i);
                if (i > 0) {
                    writer.print("|");
                }
                label.unparse(writer, 0, 0);
            }
        }

        if (where != null) {
            writer.keyword("where");
            where.unparse(writer, 0, 0);
        }
    }

    public SqlIdentifier getNameId() {
        return name;
    }

    public SqlIdentifier setName(SqlIdentifier name) {
        this.name = name;
        return this.name;
    }

    public SqlNodeList getLabels() {
        return labels;
    }

    public List<String> getLabelNames() {
        if (labels == null) {
            return Collections.emptyList();
        }
        List<String> labelNames = new ArrayList<>(labels.size());
        for (SqlNode labelNode : labels) {
            SqlIdentifier label = (SqlIdentifier) labelNode;
            labelNames.add(label.getSimple());
        }
        return labelNames;
    }

    public String getName() {
        if (name != null) {
            return name.getSimple();
        } else {
            return null;
        }
    }

    public SqlNode getWhere() {
        return where;
    }

    public void setWhere(SqlNode where) {
        this.where = where;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateQuery(this, scope, validator.getUnknownType());
    }
}
