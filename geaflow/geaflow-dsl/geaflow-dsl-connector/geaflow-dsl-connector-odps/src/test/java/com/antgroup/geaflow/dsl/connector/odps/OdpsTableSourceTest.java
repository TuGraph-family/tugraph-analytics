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

package com.antgroup.geaflow.dsl.connector.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.BinaryStringType;
import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.ByteType;
import com.antgroup.geaflow.common.type.primitive.DecimalType;
import com.antgroup.geaflow.common.type.primitive.DoubleType;
import com.antgroup.geaflow.common.type.primitive.FloatType;
import com.antgroup.geaflow.common.type.primitive.IntegerType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.common.type.primitive.TimestampType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.common.types.VoidType;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.odps.OdpsTableSource.OdpsShardPartition;
import com.antgroup.geaflow.dsl.connector.odps.utils.OdpsConnectorUtils;
import com.antgroup.geaflow.dsl.connector.odps.utils.OdpsRecordWithPartitionSpec;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

public class OdpsTableSourceTest {

    @Test
    public void testOdpsTableSource() throws IOException {
        OdpsTableSource source = new OdpsTableSource();
        Configuration config = new Configuration();
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT, "http://test.odps.com/api");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT, "test_project");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE, "test_table");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY, "test_access_key");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID, "test_access_id");
        TableSchema schema = new TableSchema(
            new TableField("src_id", IntegerType.INSTANCE, false),
            new TableField("target_id", IntegerType.INSTANCE, false),
            new TableField("relation", IntegerType.INSTANCE, false)
        );
        source.init(config, schema);
        try {
            source.open(new DefaultRuntimeContext(config));
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can't bind xml to com.aliyun.odps.Table$TableModel");
        }
        try {
            List<Partition> odpsPartitions = source.listPartitions();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot list partitions from ODPS, endPoint: http://test.odps.com/api");
        }
        Partition firstPartition = new OdpsShardPartition("prefix-", new PartitionSpec("dt='20000000'"));
        long windowSize = 100L;
        try {
            FetchData data = source.fetch(firstPartition, Optional.empty(), windowSize);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot get Odps session.");
        }
        TableDeserializer deserializer = source.getDeserializer(config);
        deserializer.init(config, schema);
        deserializer.deserialize(new OdpsRecordWithPartitionSpec(new ArrayRecord(
            new Column[]{ new Column("src_id", OdpsType.STRING), new Column("test", OdpsType.STRING)},
            new Object[]{ "16", "32" }), null));
    }

    @Test
    public void testPartitionSpec() throws IOException {
        OdpsTableSource source = new OdpsTableSource();
        Configuration config = new Configuration();
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT, "http://test.odps.com/api");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT, "test_project");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE, "test_table");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY, "test_access_key");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID, "test_access_id");
        TableSchema schema = new TableSchema(
            new TableField("src_id", IntegerType.INSTANCE, false),
            new TableField("target_id", IntegerType.INSTANCE, false),
            new TableField("relation", IntegerType.INSTANCE, false)
        );
        source.init(config, schema);
        try {
            source.open(new DefaultRuntimeContext(config));
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Can't bind xml to com.aliyun.odps.Table$TableModel");
        }
        try {
            List<Partition> odpsPartitions = source.listPartitions();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot list partitions from ODPS, endPoint: http://test.odps.com/api");
        }
        Partition firstPartition = new OdpsShardPartition("prefix-", new PartitionSpec("dt='20000000'"));
        long windowSize = 100L;
        try {
            FetchData data = source.fetch(firstPartition, Optional.empty(), windowSize);
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot get Odps session.");
        }
        TableDeserializer deserializer = source.getDeserializer(config);
        deserializer.init(config, schema);
        deserializer.deserialize(new OdpsRecordWithPartitionSpec(new ArrayRecord(
            new Column[]{ new Column("src_id", OdpsType.STRING), new Column("test", OdpsType.STRING)},
            new Object[]{ "16", "32" }), null));
    }

    @Test
    public void testOdpsConnectorUtils() {
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.SMALLINT, LongType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.INT, LongType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.FLOAT, FloatType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.DOUBLE, DoubleType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.BOOLEAN, BooleanType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.STRING, BinaryStringType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.BINARY, ByteType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.DECIMAL, DecimalType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.VOID, VoidType.INSTANCE), true);
        Assert.assertEquals(OdpsConnectorUtils.typeEquals(OdpsType.DATE, TimestampType.INSTANCE), true);
    }
}
