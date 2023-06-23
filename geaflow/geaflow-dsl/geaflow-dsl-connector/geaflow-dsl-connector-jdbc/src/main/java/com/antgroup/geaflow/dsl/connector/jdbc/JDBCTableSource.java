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

package com.antgroup.geaflow.dsl.connector.jdbc;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableSchema;
import com.antgroup.geaflow.dsl.connector.api.FetchData;
import com.antgroup.geaflow.dsl.connector.api.Offset;
import com.antgroup.geaflow.dsl.connector.api.Partition;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.serde.TableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.RowTableDeserializer;
import com.antgroup.geaflow.dsl.connector.jdbc.util.JDBCUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCTableSource implements TableSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCTableSource.class);
    private Configuration tableConf;
    private StructType schema;
    private String driver;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;

    @Override
    public void init(Configuration tableConf, TableSchema tableSchema) {
        LOGGER.info("prepare with config: {}, \n schema: {}", tableConf, tableSchema);
        this.tableConf = tableConf;
        this.schema = tableSchema;

        this.driver = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_DRIVER);
        this.url = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_URL);
        this.username = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_USERNAME);
        this.password = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_PASSWORD);
        this.tableName = tableConf.getString(JDBCConfigKeys.GEAFLOW_DSL_JDBC_TABLE_NAME);
    }

    @Override
    public void open(RuntimeContext context) {
        try {
            Class.forName(this.driver);
            this.connection = DriverManager.getConnection(url, username, password);
            this.statement = connection.createStatement();
        } catch (Exception e) {
            throw new GeaFlowDSLException("failed to connect to database", e);
        }
    }

    @Override
    public List<Partition> listPartitions() {
        return Collections.singletonList(new JDBCPartition(this.tableName));
    }

    @Override
    public <IN> TableDeserializer<IN> getDeserializer(Configuration conf) {
        return (TableDeserializer<IN>) new RowTableDeserializer();
    }

    @Override
    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset,
                                  long windowSize) throws IOException {
        if (!partition.getName().equals(this.tableName)) {
            throw new GeaFlowDSLException("wrong partition");
        }
        long offset = 0;
        if (startOffset.isPresent()) {
            offset = startOffset.get().getOffset();
        }
        List<Row> dataList;
        try {
            dataList = JDBCUtils.selectRowsFromTable(this.statement, this.tableName,
                this.schema.size(), offset, windowSize);
        } catch (SQLException e) {
            throw new GeaFlowDSLException("select rows form table failed.", e);
        }
        JDBCOffset nextOffset = new JDBCOffset(offset + windowSize);
        return (FetchData<T>) new FetchData<>(dataList, nextOffset, false);
    }

    @Override
    public void close() {
        try {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            throw new GeaFlowDSLException("failed to close connection.");
        }
    }

    public static class JDBCPartition implements Partition {

        String tableName;

        public JDBCPartition(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public String getName() {
            return tableName;
        }
    }

    public static class JDBCOffset implements Offset {

        private final long offset;

        public JDBCOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public String humanReadable() {
            return String.valueOf(offset);
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public boolean isTimestamp() {
            return false;
        }
    }
}
