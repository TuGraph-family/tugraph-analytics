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

package com.antgroup.geaflow.dsl.connector.file;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import com.antgroup.geaflow.dsl.connector.file.source.format.CsvFormat;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CsvFormatTest {

    @Test
    public void testReadSkipHeader() throws Exception {
        String output = "target/test/csv/output";
        writeData(output, "id,name,price", "1,a1,10", "2,a2,12", "3,a3,15");

        CsvFormat format = new CsvFormat();
        Map<String, String> config = new HashMap<>();
        File outputFile = new File(output);
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), outputFile.getAbsolutePath());
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_SKIP_HEADER.getKey(), "true");

        FileSplit fileSplit = new FileSplit(outputFile.getAbsolutePath());
        StructType dataSchema = new StructType(
            new TableField("price", Types.DOUBLE),
            new TableField("name", Types.BINARY_STRING)
        );
        format.init(new Configuration(config), new TableSchema(dataSchema), fileSplit);
        Iterator<Row> iterator = format.batchRead();
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (result.length() > 0) {
                result.append("\n");
            }
            result.append(row.toString());
        }
        Assert.assertEquals(result.toString(),
            "[10.0, a1]\n"
                + "[12.0, a2]\n"
                + "[15.0, a3]");
    }

    @Test
    public void testReadNoHeader() throws Exception {
        String output = "target/test/csv/output";
        writeData(output, "1,a1,10", "2,a2,12", "3,a3,15");

        CsvFormat format = new CsvFormat();
        Map<String, String> config = new HashMap<>();
        File outputFile = new File(output);
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_FILE_PATH.getKey(), outputFile.getAbsolutePath());
        config.put(ConnectorConfigKeys.GEAFLOW_DSL_SKIP_HEADER.getKey(), "false");

        FileSplit fileSplit = new FileSplit(outputFile.getAbsolutePath());
        StructType dataSchema = new StructType(
            new TableField("id", Types.INTEGER, false),
            new TableField("name", Types.BINARY_STRING),
            new TableField("price", Types.DOUBLE)
        );
        format.init(new Configuration(config), new TableSchema(dataSchema), fileSplit);
        Iterator<Row> iterator = format.batchRead();
        StringBuilder result = new StringBuilder();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (result.length() > 0) {
                result.append("\n");
            }
            result.append(row.toString());
        }
        Assert.assertEquals(result.toString(),
            "[1, a1, 10.0]\n"
                + "[2, a2, 12.0]\n"
                + "[3, a3, 15.0]");
    }

    private void writeData(String outputFile, String... lines) throws Exception {
        FileUtils.writeLines(new File(outputFile), Lists.newArrayList(lines));
    }
}
