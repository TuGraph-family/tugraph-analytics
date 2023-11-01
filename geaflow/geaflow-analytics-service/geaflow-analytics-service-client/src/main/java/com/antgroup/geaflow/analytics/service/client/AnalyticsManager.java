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

import static com.antgroup.geaflow.analytics.service.client.AnalyticsHeaders.ANALYTICS_HEADER_CLEAR_TRANSACTION_ID;
import static com.antgroup.geaflow.analytics.service.client.AnalyticsHeaders.ANALYTICS_HEADER_SET_SESSION;
import static com.antgroup.geaflow.analytics.service.client.AnalyticsHeaders.ANALYTICS_HEADER_STARTED_TRANSACTION_ID;
import static com.antgroup.geaflow.analytics.service.client.AnalyticsHeaders.ANALYTICS_HEADER_USER;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.Objects.requireNonNull;

import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.query.QueryStatusInfo;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * This class is an adaptation of Presto's com.facebook.presto.client.StatementClientV1.
 */
public class AnalyticsManager implements IAnalyticsManager {

    private static final String USER_AGENT_VALUE = AnalyticsManager.class.getSimpleName() + "/" + firstNonNull(
        AnalyticsManager.class.getPackage().getImplementationVersion(), "unknown");

    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();

    private static final String URL_PATH = "/rest/analytics/query/execute";

    private static final String QUERY = "query";

    private static final String IDENTITY = "identity";

    private static final String EMPTY = "";

    private static final String MESSAGE_DELIMITER = ": ";

    private static final int HTTP_REQUEST_OVER_CODE = 429;

    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();

    private final AtomicReference<String> startedTransactionId = new AtomicReference<>();

    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<StatementStatus> state = new AtomicReference<>(StatementStatus.RUNNING);

    private final boolean compressionDisabled;

    private final String user;

    private final OkHttpClient httpClient;

    private final String script;

    public AnalyticsManager(OkHttpClient httpClient, AnalyticsManagerSession session, String script) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(script, "query script is null");
        this.httpClient = httpClient;
        this.script = script;
        this.user = session.getUser();
        this.compressionDisabled = session.isCompressionDisabled();
        Request request = buildQueryRequest(session, script);
        // Execute request.
        JsonResponse response = JsonResponse.execute(httpClient, request);
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
            state.compareAndSet(StatementStatus.RUNNING, StatementStatus.CLIENT_ERROR);
            throw requestFailedException("starting query", request, response);
        }
        processResponse(response.getHeaders(), response.getValue());
    }

    private Request buildQueryRequest(AnalyticsManagerSession session, String script) {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new GeaflowRuntimeException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath(URL_PATH).build();
        RequestBody body = new FormBody.Builder().add(QUERY, script).build();
        Request.Builder builder = prepareRequest(url).post(body);
        Map<String, String> customHeaders = session.getCustomHeaders();
        for (Entry<String, String> entry : customHeaders.entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private Request.Builder prepareRequest(HttpUrl url) {
        Request.Builder builder = new Request.Builder()
            .addHeader(ANALYTICS_HEADER_USER, user)
            .addHeader(USER_AGENT, USER_AGENT_VALUE)
            .url(url);
        if (compressionDisabled) {
            builder.header(ACCEPT_ENCODING, IDENTITY);
        }
        return builder;
    }

    private void processResponse(Headers headers, QueryResults results) {
        for (String setSession : headers.values(ANALYTICS_HEADER_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), urlDecode(keyValue.get(1)));
        }

        String startedTransactionId = headers.get(ANALYTICS_HEADER_STARTED_TRANSACTION_ID);
        if (startedTransactionId != null) {
            this.startedTransactionId.set(startedTransactionId);
        }
        if (headers.get(ANALYTICS_HEADER_CLEAR_TRANSACTION_ID) != null) {
            clearTransactionId.set(true);
        }
        currentResults.set(results);
    }

    @Override
    public String getQuery() {
        return script;
    }

    @Override
    public boolean isRunning() {
        return state.get() == StatementStatus.RUNNING;
    }

    @Override
    public boolean isClientAborted() {
        return state.get() == StatementStatus.CLIENT_ABORTED;
    }

    @Override
    public boolean isClientError() {
        return state.get() == StatementStatus.CLIENT_ERROR;
    }

    @Override
    public boolean isFinished() {
        return state.get() == StatementStatus.FINISHED;
    }

    @Override
    public Map<String, String> getSessionProperties() {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    @Override
    public String getStartedTransactionId() {
        return startedTransactionId.get();
    }

    @Override
    public boolean isClearTransactionId() {
        return clearTransactionId.get();
    }

    @Override
    public QueryStatusInfo getCurrentStatusInfo() {
        Preconditions.checkState(isRunning(),
            "current position status info is not valid (cursor past " + "end)");
        return currentResults.get();
    }

    @Override
    public QueryStatusInfo getFinalStatusInfo() {
        Preconditions.checkState(!isRunning(), "current position status info is still valid");
        return currentResults.get();
    }

    @Override
    public QueryResults getCurrentQueryResult() {
        Preconditions.checkState(isRunning(),
            "current position status is not valid (cursor past end)");
        return currentResults.get();
    }

    @Override
    public void close() {
        state.compareAndSet(StatementStatus.RUNNING, StatementStatus.CLIENT_ABORTED);
    }

    private static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private enum StatementStatus {
        /**
         * Client running status.
         */
        RUNNING,

        /**
         * Client error status.
         */
        CLIENT_ERROR,

        /**
         * Client aborted status.
         */
        CLIENT_ABORTED,

        /**
         * Client finished status.
         */
        FINISHED,
    }

    private RuntimeException requestFailedException(String task, Request request,
                                                    JsonResponse response) {
        if (!response.hasValue()) {
            if (response.getStatusCode() == HTTP_UNAUTHORIZED) {
                return new GeaflowRuntimeException(
                    "GeaFlow Analytics Client Authentication failed" + Optional.ofNullable(
                            response.getStatusMessage()).map(message -> MESSAGE_DELIMITER + message).orElse(EMPTY));
            }
            if (response.getStatusCode() == HTTP_REQUEST_OVER_CODE) {
                return new GeaflowRuntimeException(
                    "GeaFlow Analytics Client Request throttled " + Optional.ofNullable(
                            response.getStatusMessage()).map(message -> MESSAGE_DELIMITER + message).orElse(EMPTY));
            }
            byte[] responseBytes = response.getResponseBytes();
            return new GeaflowRuntimeException(
                String.format("GeaFlow Analytics Client Error %s at %s returned an invalid "
                        + "response: %s [Error: %s]", task,
                    request.url(), response, new String(responseBytes, StandardCharsets.UTF_8)), response.getException());
        }
        return new GeaflowRuntimeException(
            String.format("GeaFlow Analytics Client Error %s at %s returned HTTP %s", task,
                request.url(), response.getStatusCode()));
    }

}
