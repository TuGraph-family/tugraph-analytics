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

package com.antgroup.geaflow.dsl.runtime.query;

import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.dsl.runtime.testenv.SourceFunctionNoPartitionCheck;
import java.util.HashMap;
import java.util.Map;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class JDBCTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTest.class);

    private final String URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private final String username = "h2_user";
    private final String password = "h2_pwd";

    @BeforeClass
    public void setup() {
        LOGGER.info("start h2 database.");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(this.URL);
        dataSource.setURL(this.username);
        dataSource.setPassword(this.password);
    }

    @Test
    public void testJDBC_001() throws Exception {
        Map<String, String> config  = new HashMap<>();
        config.put(DSLConfigKeys.GEAFLOW_DSL_WINDOW_SIZE.getKey(), String.valueOf(1L));
        config.put(DSLConfigKeys.GEAFLOW_DSL_CUSTOM_SOURCE_FUNCTION.getKey(),
            SourceFunctionNoPartitionCheck.class.getName());
        QueryTester tester = QueryTester
            .build()
            .withQueryPath("/query/jdbc_write_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute()
            .withQueryPath("/query/jdbc_scan_001.sql")
            .withConfig(config)
            .withTestTimeWaitSeconds(60)
            .execute();

        tester.checkSinkResult();
    }
}
