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

package com.antgroup.geaflow.dsl.connector.file.source.format;

import com.antgroup.geaflow.common.binary.BinaryString;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.type.Types;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.ArrayType;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.file.FileConnectorUtil;
import com.antgroup.geaflow.dsl.connector.file.source.FileTableSource.FileSplit;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.TypeBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFormat implements FileFormat<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFormat.class);

    private StructType dataSchema;

    private List<InputSplit> inputSplits;

    private ParquetInputFormat<GenericData.Record> inputFormat;

    private TaskAttemptContext taskAttemptContext;

    private RecordReader<Void, GenericData.Record> currentReader;

    @Override
    public String getFormat() {
        return "parquet";
    }

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema, FileSplit split) throws IOException {
        this.dataSchema = tableSchema.getDataSchema();
        this.inputFormat = new AvroParquetInputFormat<>();
        Job job = Job.getInstance(FileConnectorUtil.toHadoopConf(tableConf));
        Path path = new Path(split.getPath());
        path = path.getFileSystem(job.getConfiguration()).makeQualified(path);
        LOGGER.info("Read parquet from: {}", path);
        AvroParquetInputFormat.setInputPaths(job, path);

        Schema avroSchema = convertToAvroSchema(dataSchema, false);
        job.getConfiguration().set(AvroReadSupport.AVRO_COMPATIBILITY, "false");
        AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);
        AvroParquetInputFormat.setRequestedProjection(job, avroSchema);

        JobContext jobContext = new JobContextImpl(job.getConfiguration(), new JobID());
        this.inputSplits = inputFormat.getSplits(jobContext);
        this.taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
    }

    @Override
    public Iterator<Row> batchRead() throws IOException {
        return new Iterator<Row>() {

            private int index = 0;

            @Override
            public boolean hasNext() {
                try {
                    boolean hasNext = currentReader != null && currentReader.nextKeyValue();
                    if (currentReader == null || !hasNext) {
                        if (index < inputSplits.size()) {
                            InputSplit split = inputSplits.get(index);
                            // close previous reader
                            if (currentReader != null) {
                                currentReader.close();
                            }
                            // create new reader
                            currentReader = inputFormat.createRecordReader(split, taskAttemptContext);
                            currentReader.initialize(split, taskAttemptContext);
                            hasNext = currentReader.nextKeyValue();
                        } else {
                            return false;
                        }
                        index++;
                    }
                    return hasNext;
                } catch (Exception e) {
                    throw new GeaFlowDSLException(e);
                }
            }

            @Override
            public Row next() {
                try {
                    GenericData.Record record = currentReader.getCurrentValue();
                    return convertAvroRecordToRow(record);
                } catch (Exception e) {
                    throw new GeaFlowDSLException(e);
                }
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (currentReader != null) {
            currentReader.close();
        }
    }

    @Override
    public TableDeserializer<Row> getDeserializer() {
        return null;
    }

    public static Schema convertToAvroSchema(IType<?> sqlType, boolean nullable) {
        TypeBuilder<Schema> builder = SchemaBuilder.builder();
        Schema avroType;
        switch (sqlType.getName()) {
            case Types.TYPE_NAME_BINARY_STRING:
            case Types.TYPE_NAME_STRING:
                avroType = builder.stringType();
                break;
            case Types.TYPE_NAME_INTEGER:
                avroType = builder.intType();
                break;
            case Types.TYPE_NAME_LONG:
                avroType = builder.longType();
                break;
            case Types.TYPE_NAME_BOOLEAN:
                avroType = builder.booleanType();
                break;
            case Types.TYPE_NAME_DOUBLE:
                avroType = builder.doubleType();
                break;
            case Types.TYPE_NAME_TIMESTAMP:
                avroType = LogicalTypes.timestampMicros().addToSchema(builder.longType());
                break;
            case Types.TYPE_NAME_STRUCT:
                StructType structType = (StructType) sqlType;
                FieldAssembler<Schema> fieldAssembler = builder.record("struct").namespace("").fields();
                for (TableField field : structType.getFields()) {
                    Schema fieldAvroType = convertToAvroSchema(field.getType(), field.isNullable());
                    fieldAssembler.name(field.getName()).type(fieldAvroType).noDefault();
                }
                avroType = fieldAssembler.endRecord();
                break;
            case Types.TYPE_NAME_ARRAY:
                ArrayType arrayType = (ArrayType) sqlType;
                avroType = builder.array().items(convertToAvroSchema(arrayType.getComponentType(), nullable));
                break;
            default:
                throw new GeaFlowDSLException("Not support type: {}", sqlType.getName());
        }
        if (nullable) {
            Schema nullSchema = builder.nullType();
            return Schema.createUnion(avroType, nullSchema);
        }
        return avroType;
    }

    private Row convertAvroRecordToRow(GenericData.Record record) {
        Object[] fields = new Object[dataSchema.size()];
        for (int i = 0; i < fields.length; i++) {
            IType<?> type = dataSchema.getType(i);
            switch (type.getName()) {
                case Types.TYPE_NAME_BINARY_STRING:
                    Utf8 utf8 = (Utf8) record.get(i);
                    fields[i] = utf8 == null ? null : BinaryString.fromBytes(utf8.getBytes());
                    break;
                case Types.TYPE_NAME_INTEGER:
                    fields[i] = (Integer) record.get(i);
                    break;
                case Types.TYPE_NAME_LONG:
                    fields[i] = (Long) record.get(i);
                    break;
                case Types.TYPE_NAME_DOUBLE:
                    fields[i] = (Double) record.get(i);
                    break;
                case Types.TYPE_NAME_TIMESTAMP:
                    fields[i] = record.get(i);
                    break;
                case Types.TYPE_NAME_BOOLEAN:
                    fields[i] = (Boolean) record.get(i);
                    break;
                default:
                    throw new GeaFlowDSLException("Not support type: {}", type.getName());
            }
        }
        return ObjectRow.create(fields);
    }
}
