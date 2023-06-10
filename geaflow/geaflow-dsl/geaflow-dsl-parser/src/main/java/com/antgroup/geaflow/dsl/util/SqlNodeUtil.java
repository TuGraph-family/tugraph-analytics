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

import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

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
}
