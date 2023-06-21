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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.util.SqlVisitor;

public class SqlNodeUtil {

    public static void unparseNodeList(SqlWriter writer,
                                       SqlNodeList nodeList, String sep) {
        for (int i = 0; i < nodeList.size(); i++) {
            if (i > 0) {
                writer.print(sep);
                writer.newlineAndIndent();
            }
            nodeList.get(i).unparse(writer, 0, 0);
        }
    }

    public static List<SqlUnresolvedFunction> findUnresolvedFunctions(SqlNode sqlNode) {
        return sqlNode.accept(new SqlVisitor<List<SqlUnresolvedFunction>>() {
            @Override
            public List<SqlUnresolvedFunction> visit(SqlLiteral literal) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlCall call) {
                List<SqlUnresolvedFunction> functions = new ArrayList<>();
                if (call.getOperator() instanceof SqlUnresolvedFunction) {
                    functions.add((SqlUnresolvedFunction) call.getOperator());
                }
                functions.addAll(visitNodes(call.getOperandList()));
                return functions;
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlNodeList nodeList) {
                return visitNodes(nodeList.getList());
            }

            private List<SqlUnresolvedFunction> visitNodes(List<SqlNode> nodes) {
                if (nodes == null) {
                    return Collections.emptyList();
                }
                return nodes.stream()
                    .flatMap(node -> {
                        if (node != null) {
                            return node.accept(this).stream();
                        } else {
                            return Collections.<SqlUnresolvedFunction>emptyList().stream();
                        }
                    })
                    .collect(Collectors.toList());
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlIdentifier id) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlDataTypeSpec type) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlDynamicParam param) {
                return Collections.emptyList();
            }

            @Override
            public List<SqlUnresolvedFunction> visit(SqlIntervalQualifier intervalQualifier) {
                return Collections.emptyList();
            }
        });
    }
}
