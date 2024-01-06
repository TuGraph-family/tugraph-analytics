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

package com.antgroup.geaflow.analytics.service.server.http.handler;

import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.analytics.service.query.QueryError;
import com.antgroup.geaflow.analytics.service.query.QueryIdGenerator;
import com.antgroup.geaflow.analytics.service.query.QueryInfo;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.StandardError;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpAnalyticsServiceHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAnalyticsServiceHandler.class);

    private static final String QUERY = "query";
    public static final String QUERY_RESULT = "query_result";

    private final BlockingQueue<QueryInfo> requestBlockingQueue;
    private final BlockingQueue<QueryResults> responseBlockingQueue;
    private final QueryIdGenerator queryIdGenerator;
    private final Semaphore semaphore;

    public HttpAnalyticsServiceHandler(BlockingQueue<QueryInfo> requestBlockingQueue,
                                       BlockingQueue<QueryResults> responseBlockingQueue,
                                       Semaphore semaphore) {
        this.requestBlockingQueue = requestBlockingQueue;
        this.responseBlockingQueue = responseBlockingQueue;
        this.semaphore = semaphore;
        this.queryIdGenerator = new QueryIdGenerator();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JSONObject ret = new JSONObject();
        String queryId = queryIdGenerator.createQueryId();
        try {
            if (!this.semaphore.tryAcquire()) {
                QueryError queryError = StandardError.ANALYTICS_SERVER_BUSY.getQueryError();
                QueryResults queryResults = new QueryResults(queryId, queryError);
                ret.put(QUERY_RESULT, queryResults);
                resp.setStatus(HttpServletResponse.SC_OK);
            } else {
                String query = req.getParameter(QUERY);
                QueryInfo queryInfo = new QueryInfo(queryId, query);
                LOGGER.info("start execute query [{}]", queryInfo);
                long start = System.currentTimeMillis();
                requestBlockingQueue.put(queryInfo);
                QueryResults queryResults = responseBlockingQueue.take();
                ret.put(QUERY_RESULT, queryResults);
                LOGGER.info("finish execute query [{}], result {}, cost {}ms", query, queryResults, System.currentTimeMillis() - start);
                resp.setStatus(HttpServletResponse.SC_OK);
            }
        } catch (Throwable t) {
            QueryResults queryResults = new QueryResults(queryId, new QueryError(t.getMessage()));
            ret.put(ERROR_KEY, queryResults);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } finally {
            addHeader(resp);
            byte[] resultBytes = ret.toJSONString().getBytes(StandardCharsets.UTF_8);
            resp.getOutputStream().write(resultBytes);
            resp.getOutputStream().flush();
            this.semaphore.release();
        }
    }

}
