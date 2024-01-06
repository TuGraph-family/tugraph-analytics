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

package com.antgroup.geaflow.analytics.service.query;

import java.io.Serializable;

/**
 * This class is an adaptation of Presto's com.facebook.presto.client.QueryResults.
 */
public class QueryResults implements QueryStatusInfo, Serializable {

    private static final String DEFAULT_QUERY_ID = "0";

    private static final String SEPARATOR = ":";

    private final String queryId;
    private final Object data;
    private final QueryError error;
    private final boolean queryStatus;

    public QueryResults(String queryId, Object data, QueryError error, boolean queryStatus) {
        this.queryId = queryId;
        this.data = data;
        this.error = error;
        this.queryStatus = queryStatus;
    }

    public QueryResults(String queryId, Object data) {
        this(queryId, data, null, true);
    }

    public QueryResults(String queryId, QueryError error) {
        this(queryId, null, error, false);
    }

    public QueryResults(QueryError error) {
        this(DEFAULT_QUERY_ID, null, error, false);
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    public Object getData() {
        return data;
    }

    public QueryError getError() {
        return error;
    }

    public boolean isQueryStatus() {
        return queryStatus;
    }

    @Override
    public String toString() {
        return this.queryId + SEPARATOR + this.isQueryStatus() + SEPARATOR + (this.isQueryStatus() ? this.data : this.error.toString());
    }
}
