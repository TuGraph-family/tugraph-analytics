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
import com.antgroup.geaflow.pipeline.service.ServiceType;
import java.io.Closeable;

public interface IQueryRunner extends Closeable {

    /**
     * Init query runner.
     */
    void init(QueryRunnerContext handlerContext);

    /**
     * Execute query.
     */
    QueryResults executeQuery(String queryScript);

    /**
     * Get service type.
     */
    ServiceType getServiceType();

    /**
     * Cancel query.
     */
    QueryResults cancelQuery(long queryId);

    /**
     * Query runner is running.
     */
    boolean isRunning();

    /**
     * Query runner is aborted, when request fail.
     */
    boolean isAborted();

    /**
     * Query runner is error.
     */
    boolean isError();

    /**
     * Query runner is finished.
     */
    boolean isFinished();
}
