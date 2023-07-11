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

package com.antgroup.geaflow.dsl.common.types;

import java.util.ArrayList;
import java.util.List;

public class TableSchema extends StructType {

    private final StructType dataSchema;
    private final StructType partitionSchema;

    public TableSchema(StructType dataSchema, StructType partitionSchema) {
        super(combine(dataSchema, partitionSchema));
        this.dataSchema = dataSchema;
        this.partitionSchema = partitionSchema;
    }

    public TableSchema(StructType dataSchema) {
        this(dataSchema, new StructType());
    }

    public TableSchema(List<TableField> fields) {
        super(fields);
        this.dataSchema = new StructType(fields);
        this.partitionSchema = new StructType();
    }

    public TableSchema(TableField... fields) {
        super(fields);
        this.dataSchema = new StructType(fields);
        this.partitionSchema = new StructType();
    }

    private static List<TableField> combine(StructType dataSchema, StructType partitionSchema) {
        List<TableField> fields = new ArrayList<>();
        fields.addAll(dataSchema.getFields());
        fields.addAll(partitionSchema.getFields());
        return fields;
    }

    public StructType getDataSchema() {
        return dataSchema;
    }

    public StructType getPartitionSchema() {
        return partitionSchema;
    }

    @Override
    public StructType addField(TableField field) {
        throw new IllegalArgumentException("addField not support");
    }

    @Override
    public StructType replace(String name, TableField newField) {
        throw new IllegalArgumentException("replace not support");
    }
}
