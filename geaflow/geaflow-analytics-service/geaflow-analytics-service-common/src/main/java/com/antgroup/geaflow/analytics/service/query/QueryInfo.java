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

public class QueryInfo {

    private final String queryId;

    private final String queryScript;

    public QueryInfo(String queryId, String queryScript) {
        this.queryId = queryId;
        this.queryScript = queryScript;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getQueryScript() {
        return queryScript;
    }

    @Override
    public String toString() {
        return "queryId: " + queryId + " queryScript: " + queryScript;
    }
}
