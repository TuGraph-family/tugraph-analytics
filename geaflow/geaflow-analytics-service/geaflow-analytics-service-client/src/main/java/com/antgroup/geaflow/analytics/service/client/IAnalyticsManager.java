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

package com.antgroup.geaflow.analytics.service.client;

import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.QueryStatusInfo;
import java.io.Closeable;
import java.util.Map;

public interface IAnalyticsManager extends Closeable {

    /**
     * Get query script.
     */
    String getQuery();

    /**
     * Enable running.
     */
    boolean isRunning();

    /**
     * Enable aborted.
     */
    boolean isClientAborted();

    /**
     * Enable error.
     */
    boolean isClientError();

    /**
     * Enable finished.
     */
    boolean isFinished();

    /**
     * Get session properties.
     */
    Map<String, String> getSessionProperties();

    /**
     * Get started transaction id.
     */
    String getStartedTransactionId();

    /**
     * Enable clear transaction id.
     */
    boolean isClearTransactionId();

    /**
     * Get current query status info.
     */
    QueryStatusInfo getCurrentStatusInfo();

    /**
     * Get finial query status info.
     */
    QueryStatusInfo getFinalStatusInfo();

    /**
     * Get current query result.
     */
    QueryResults getCurrentQueryResult();

    /**
     * Close statement.
     */
    @Override
    void close();

}
