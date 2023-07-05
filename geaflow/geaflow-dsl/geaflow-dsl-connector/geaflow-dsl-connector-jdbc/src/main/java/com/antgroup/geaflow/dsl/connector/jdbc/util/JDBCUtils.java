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

package com.antgroup.geaflow.dsl.connector.jdbc.util;

import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.Windows;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;

public class JDBCUtils {

    public static final int VARCHAR_MAXLENGTH = 255;

    private static String tableFieldToSQL(TableField tableField) {
        String typeName = tableField.getType().getName();
        switch (typeName) {
            case Types.TYPE_NAME_STRING:
            case Types.TYPE_NAME_BINARY_STRING:
                typeName = SqlTypeName.VARCHAR.getName();
                break;
            case Types.TYPE_NAME_LONG:
                typeName = SqlTypeName.BIGINT.getName();
                break;
            default:
                typeName = typeName.toUpperCase();
        }
        return String.format("%s %s%s %s", tableField.getName(), typeName,
            typeName.equals(SqlTypeName.VARCHAR.getName()) ? "(" + VARCHAR_MAXLENGTH + ")" : "",
            tableField.isNullable() ? "NULL" : "NOT " + "NULL");
    }

    public static void createTemporaryTable(Statement statement, String tableName,
                                            List<TableField> fields) throws SQLException {
        StringBuilder tableFields = new StringBuilder();
        for (TableField field : fields) {
            tableFields.append(tableFieldToSQL(field)).append(",\n");
        }
        tableFields.deleteCharAt(tableFields.lastIndexOf(","));
        String createTableQuery = String.format("CREATE TEMPORARY TABLE %s (\n%s);", tableName,
            tableFields);
        statement.execute(createTableQuery);
    }

    public static void insertIntoTable(Statement statement, String tableName,
                                       List<TableField> fields, Row row) throws SQLException {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = row.getField(i, fields.get(i).getType());
        }
        String insertIntoValues = StringUtils.join(
            Arrays.stream(values).map(value -> "'" + value + "'").collect(Collectors.toList()),
            ",");
        String insertIntoTableQuery = String.format("INSERT INTO %s VALUES (%s);", tableName,
            insertIntoValues);
        statement.execute(insertIntoTableQuery);
    }

    public static List<Row> selectRowsFromTable(Statement statement, String tableName,
                                                String whereClause, int columnNum, long startOffset,
                                                long windowSize) throws SQLException {
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            windowSize = Integer.MAX_VALUE;
        } else if (windowSize <= 0) {
            throw new GeaFlowDSLException("wrong windowSize");
        }
        String selectRowsFromTableQuery = String.format("SELECT * FROM %s %s OFFSET %d ROWS\n"
            + "FETCH NEXT (%d) ROWS ONLY;", tableName, whereClause, startOffset, windowSize);
        ResultSet resultSet = statement.executeQuery(selectRowsFromTableQuery);
        List<Row> rowList = new ArrayList<>();
        while (resultSet.next()) {
            Object[] values = new Object[columnNum];
            for (int i = 1; i <= columnNum; i++) {
                values[i - 1] = resultSet.getObject(i);
            }
            rowList.add(ObjectRow.create(values));
        }
        resultSet.close();
        return rowList;
    }
}
