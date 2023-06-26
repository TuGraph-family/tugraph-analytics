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

package com.antgroup.geaflow.dsl.connector.hive;

import com.antgroup.geaflow.common.utils.ClassUtil;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.Windows;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.hive.HiveTableSource.HiveOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

public class HiveReader {

    private final RecordReader<Writable, Writable> recordReader;
    private final StructType readSchema;
    private final Deserializer deserializer;

    private final Map<String, StructField> name2Fields = new HashMap<>();

    public HiveReader(RecordReader<Writable, Writable> recordReader, StructType readSchema,
                      StorageDescriptor sd, Properties tableProps) {
        this.recordReader = recordReader;
        this.readSchema = new StructType(readSchema.getFields().stream().map(
            f -> new TableField(f.getName().toLowerCase(Locale.ROOT), f.getType(), f.isNullable()))
            .collect(Collectors.toList()));
        this.deserializer = ClassUtil.newInstance(sd.getSerdeInfo().getSerializationLib());
        try {
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            SerDeUtils.initializeSerDe(deserializer, conf, tableProps, null);
            StructObjectInspector structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                name2Fields.put(field.getFieldName(), field);
            }
        } catch (SerDeException e) {
            throw new GeaFlowDSLException(e);
        }
    }

    public FetchData<Row> read(long windowSize, String[] partitionValues) throws Exception {
        Writable key = recordReader.createKey();
        Writable value = recordReader.createValue();
        StructObjectInspector structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();

        List<Row> rows = new ArrayList<>();
        if (windowSize == Windows.SIZE_OF_ALL_WINDOW) {
            while (recordReader.next(key, value)) {
                Object hiveRowStruct = deserializer.deserialize(value);
                Object[] values = convertHiveStructToRow(hiveRowStruct, structObjectInspector);
                if (partitionValues.length > 0) { // append partition values.
                    Object[] valueWithPartitions = new Object[values.length + partitionValues.length];
                    System.arraycopy(values, 0, valueWithPartitions, 0, values.length);
                    System.arraycopy(partitionValues, 0, valueWithPartitions,
                        values.length, partitionValues.length);
                    values = valueWithPartitions;
                }
                rows.add(ObjectRow.create(values));
            }
        } else {
            throw new GeaFlowDSLException("Cannot support stream read for hive");
        }
        return new FetchData<>(rows, new HiveOffset(recordReader.getPos()), true);
    }

    private Object[] convertHiveStructToRow(Object hiveRowStruct, StructObjectInspector structObjectInspector) {
        Object[] values = new Object[readSchema.size()];
        for (int i = 0; i < values.length; i++) {
            String fieldName = readSchema.getField(i).getName();
            StructField field = name2Fields.get(fieldName);
            if (field != null) {
                values[i] = toSqlValue(
                    structObjectInspector.getStructFieldData(hiveRowStruct, field),
                    field.getFieldObjectInspector());
            } else {
                values[i] = null;
            }
        }
        return values;
    }


    private Object toSqlValue(Object hiveValue, ObjectInspector fieldInspector) {
        if (fieldInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) fieldInspector;
            return primitiveObjectInspector.getPrimitiveJavaObject(hiveValue);
        }
        throw new GeaFlowDSLException("Complex type:{} have not support", fieldInspector.getTypeName());
    }
}
