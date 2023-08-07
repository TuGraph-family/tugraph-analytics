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

package com.antgroup.geaflow.dsl.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

public class GQLEdgeConstraint extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("GQLEdgeConstraint",
        SqlKind.OTHER_DDL);

    private SqlNodeList sourceVertexType;
    private SqlNodeList targetVertexType;

    public GQLEdgeConstraint(SqlNodeList sourceVertexType,
                             SqlNodeList targetVertexType, SqlParserPos pos) {
        super(pos);
        this.sourceVertexType = Objects.requireNonNull(sourceVertexType);
        this.targetVertexType = Objects.requireNonNull(targetVertexType);
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(sourceVertexType, targetVertexType);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.sourceVertexType = (SqlNodeList) operand;
                break;
            case 1:
                this.targetVertexType = (SqlNodeList) operand;
                break;
            default:
                throw new IndexOutOfBoundsException("current index " + i + " out of range " + 2);
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        for (int i = 0; i < sourceVertexType.size(); i++) {
            if (i > 0) {
                writer.print("|");
            }
            sourceVertexType.get(i).unparse(writer, 0, 0);
        }
        writer.print("->");
        for (int i = 0; i < targetVertexType.size(); i++) {
            if (i > 0) {
                writer.print("|");
            }
            targetVertexType.get(i).unparse(writer, 0, 0);
        }
    }

    public List<String> getSourceVertexTypes() {
        List<String> sourceTypes = new ArrayList<>();
        for (SqlNode node : sourceVertexType) {
            assert node instanceof SqlIdentifier;
            sourceTypes.add(((SqlIdentifier) node).getSimple());
        }
        return sourceTypes;
    }

    public List<String> getTargetVertexTypes() {
        List<String> targetTypes = new ArrayList<>();
        for (SqlNode node : targetVertexType) {
            assert node instanceof SqlIdentifier;
            targetTypes.add(((SqlIdentifier) node).getSimple());
        }
        return targetTypes;
    }
}
