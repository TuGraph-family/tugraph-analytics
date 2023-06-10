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

package com.antgroup.geaflow.dsl.connector.api.serde.impl;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class TextDeserializer implements TableDeserializer<String> {

    private String lineSeparator;

    private String columnSeparator;

    private boolean isColumnTrim;

    private StructType schema;

    @Override
    public void init(Configuration conf, StructType schema) {
        this.lineSeparator = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_LINE_SEPARATOR);
        this.columnSeparator = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_SEPARATOR);
        this.isColumnTrim = conf.getBoolean(ConnectorConfigKeys.GEAFLOW_DSL_COLUMN_TRIM);
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    public List<Row> deserialize(String text) {
        if (text == null || text.isEmpty()) {
            return Collections.emptyList();
        }
        List<Row> rows = new ArrayList<>();
        String[] lines = StringUtils.splitByWholeSeparator(text, lineSeparator);
        for (String line : lines) {
            if (line.isEmpty() && schema.size() >= 1) {
                continue;
            }
            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, columnSeparator);
            if (schema.size() != fields.length) {
                throw new GeaFlowDSLException("Data fields size:{}, is not equal to the schema size:{}",
                    fields.length, schema.size());
            }
            Object[] values = new Object[schema.size()];
            for (int i = 0; i < values.length; i++) {
                String trimField = isColumnTrim ? StringUtils.trim(fields[i]) : fields[i];
                values[i] = TypeCastUtil.cast(trimField, schema.getType(i));
            }
            rows.add(ObjectRow.create(values));
        }
        return rows;
    }
}
