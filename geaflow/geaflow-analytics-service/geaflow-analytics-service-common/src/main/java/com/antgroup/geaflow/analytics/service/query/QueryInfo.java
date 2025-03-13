/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.analytics.service.query;

import org.apache.calcite.rel.type.RelDataType;

public class QueryInfo {

    private final String queryId;

    private final String queryScript;

    private RelDataType scriptSchema;

    public QueryInfo(String queryId, String queryScript) {
        this.queryId = queryId;
        this.queryScript = queryScript;
    }

    public RelDataType getScriptSchema() {
        return scriptSchema;
    }

    public void setScriptSchema(RelDataType scriptSchema) {
        this.scriptSchema = scriptSchema;
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
