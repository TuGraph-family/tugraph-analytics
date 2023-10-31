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

public enum StandardError {

    /**
     * Analytics server un available.
     */
    ANALYTICS_SERVER_UNAVAILABLE(1001),

    /**
     * Analytics server busy.
     */
    ANALYTICS_SERVER_BUSY(1002),

    /**
     * Analytics no available coordinator.
     */
    ANALYTICS_NO_COORDINATOR(1003),

    /**
     * Analytics query result is null.
     */
    ANALYTICS_NULL_RESULT(1004),

    /**
     * Analytics rpc error.
     */
    ANALYTICS_RPC_ERROR(1005),

    /**
     * Analytics query result is too long.
     */
    ANALYTICS_RESULT_TO_LONG(1006)
    ;

    private final QueryError errorCode;

    StandardError(int code) {
        errorCode = new QueryError(name(), code);
    }

    public QueryError getQueryError() {
        return errorCode;
    }
}
