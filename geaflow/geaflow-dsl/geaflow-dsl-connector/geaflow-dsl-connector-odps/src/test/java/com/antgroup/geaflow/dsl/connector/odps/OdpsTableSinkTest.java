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

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.runtime.core.context.DefaultRuntimeContext;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OdpsTableSinkTest {

    @Test
    public void testOdpsTableSink() throws IOException {
        OdpsTableSink sink = new OdpsTableSink();
        Configuration config = new Configuration();
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ENDPOINT, "http://test.odps.com/api");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PROJECT, "test_project");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_TABLE, "test_table");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_KEY, "test_access_key");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_ACCESS_ID, "test_access_id");
        config.put(OdpsConfigKeys.GEAFLOW_DSL_ODPS_PARTITION_SPEC, "dt='20000000'");
        TableSchema schema = new TableSchema(
            new TableField("src_id", StringType.INSTANCE, false),
            new TableField("target_id", StringType.INSTANCE, false),
            new TableField("relation", StringType.INSTANCE, false)
        );
        sink.init(config, schema);
        try {
            sink.open(new DefaultRuntimeContext(config));
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Cannot list partitions from ODPS, endPoint: http://test.odps.com/api");
        }
        sink.write(ObjectRow.create(new Object[]{"1", "2", "3"}));
        try {
            sink.finish();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "java.lang.IllegalArgumentException");
        }
        sink.close();
    }

}
