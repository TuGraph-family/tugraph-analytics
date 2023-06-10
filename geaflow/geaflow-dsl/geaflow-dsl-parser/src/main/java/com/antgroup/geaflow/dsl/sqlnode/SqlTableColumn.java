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

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.util.SqlTypeUtil;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlTableColumn extends SqlCall {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("Table Column", SqlKind.OTHER_DDL);

    private SqlIdentifier name;
    private SqlDataTypeSpec type;
    private SqlIdentifier category;

    public SqlTableColumn(SqlIdentifier name,
                          SqlDataTypeSpec type,
                          SqlIdentifier category,
                          SqlParserPos pos) {
        super(pos);
        this.name = name;
        this.type = type;
        this.category = category;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(getName(), getType(), category);
    }

    @Override
    public void setOperand(int i, SqlNode operand) {
        switch (i) {
            case 0:
                this.name = (SqlIdentifier) operand;
                break;
            case 1:
                this.type = (SqlDataTypeSpec) operand;
                break;
            case 2:
                this.category = (SqlIdentifier) operand;
                break;
            default:
                throw new IllegalArgumentException("Illegal index: " + i);
        }
    }

    @Override
    public void unparse(SqlWriter writer,
                        int leftPrec,
                        int rightPrec) {

        name.unparse(writer, leftPrec, rightPrec);
        writer.print(" ");
        type.unparse(writer, leftPrec, rightPrec);
        if (category != null) {
            ColumnCategory category = getCategory();
            writer.print(" ");
            writer.keyword(category.name);
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public void setName(SqlIdentifier name) {
        this.name = name;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    public ColumnCategory getCategory() {
        if (category == null) {
            return ColumnCategory.NONE;
        }
        return ColumnCategory.of(category.getSimple());
    }

    public TableField toTableField() {
        String columnName = name.getSimple();

        IType<?> columnType = SqlTypeUtil.convertType(type);
        Boolean nullable = type.getNullable();
        if (nullable == null) {
            nullable = true;
        }
        return new TableField(columnName, columnType, nullable);
    }

    public enum ColumnCategory {
        NONE(""),
        ID("ID"),
        SOURCE_ID("SOURCE ID"),
        DESTINATION_ID("DESTINATION ID"),
        TIMESTAMP("TIMESTAMP");

        private final String name;

        ColumnCategory(String name) {
            this.name = Objects.requireNonNull(name);
        }

        public String getName() {
            return name;
        }

        public static ColumnCategory of(String name) {
            for (ColumnCategory category : values()) {
                if (category.name.equalsIgnoreCase(name)) {
                    return category;
                }
            }
            return NONE;
        }
    }
}
