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

package com.antgroup.geaflow.analytics.service.client.jdbc;

import static com.antgroup.geaflow.analytics.service.client.jdbc.AnalyticsResultSet.resultsException;
import static java.util.Objects.requireNonNull;

import com.antgroup.geaflow.analytics.service.client.IAnalyticsManager;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.QueryStatusInfo;
import com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AnalyticsStatement implements Statement {

    private final AtomicReference<AnalyticsConnection> connection;
    private final AtomicInteger queryTimeoutSeconds = new AtomicInteger();
    private final AtomicReference<AnalyticsResultSet> currentResult = new AtomicReference<>();
    private final AtomicReference<IAnalyticsManager> executingClient = new AtomicReference<>();

    public AnalyticsStatement(AnalyticsConnection connection) {
        this.connection = new AtomicReference<>(requireNonNull(connection, "connection is null"));
    }

    @Override
    public AnalyticsResultSet executeQuery(String statement) throws SQLException {
        if (!execute(statement)) {
            throw new GeaflowRuntimeException("Execute statement is not a query: " + statement);
        }
        return currentResult.get();
    }

    @Override
    public boolean execute(String statement) throws SQLException {
        return executeQueryInternal(statement);
    }

    @Override
    public int executeUpdate(String query) {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    private boolean executeQueryInternal(String statement) throws SQLException {
        // Remove current results.
        removeCurrentResults();
        // Check connection.
        checkConnect();
        IAnalyticsManager client = null;
        AnalyticsResultSet resultSet = null;
        try {
            AnalyticsConnection connection = connection();
            client = connection.executeQuery(statement, getStatementSessionProperties());
            if (client.isFinished()) {
                QueryStatusInfo finalStatusInfo = client.getFinalStatusInfo();
                if (finalStatusInfo.getError() != null) {
                    throw resultsException(finalStatusInfo);
                }
            }
            executingClient.set(client);
            QueryResults queryResults = client.getCurrentQueryResult();
            resultSet = new AnalyticsResultSet(this, client, queryResults);
            currentResult.set(resultSet);
            return true;
        } catch (Exception e) {
            throw new GeaflowRuntimeException("execute query fail", e);
        } finally {
            executingClient.set(null);
            if (currentResult.get() == null) {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    private void removeCurrentResults() throws SQLException {
        AnalyticsResultSet resultSet = currentResult.getAndSet(null);
        if (resultSet != null) {
            resultSet.close();
        }
    }

    private void checkConnect() throws SQLException {
        connection();
    }

    private Map<String, String> getStatementSessionProperties() {
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        if (queryTimeoutSeconds.get() > 0) {
            sessionProperties.put(FrameworkConfigKeys.CLIENT_QUERY_TIMEOUT.getKey(),
                queryTimeoutSeconds.get() + "s");
        }
        return sessionProperties.build();
    }

    private AnalyticsConnection connection() throws SQLException {
        AnalyticsConnection connection = this.connection.get();
        if (connection == null || connection.isClosed()) {
            throw new GeaflowRuntimeException("analytics connection is closed");
        }
        return connection;
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkConnect();
        if (seconds < 0) {
            throw new GeaflowRuntimeException("Query timeout seconds must be positive");
        }
        queryTimeoutSeconds.set(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        checkConnect();
        IAnalyticsManager statementClient = executingClient.get();
        if (statementClient != null) {
            statementClient.close();
        }
        removeCurrentResults();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String query) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String query, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String query, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String query, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String query, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String query, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute(String query, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

}
