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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.net.HttpHeaders.LOCATION;
import static java.util.Objects.requireNonNull;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.antgroup.geaflow.analytics.service.query.QueryResults;
import com.antgroup.geaflow.analytics.service.server.http.handler.HttpAnalyticsServiceHandler;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Presto's com.facebook.presto.client.JsonResponse.
 */
public class JsonResponse {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonResponse.class);

    private static final int TEMP_REDIRECT = 307;
    private static final int PERMANENT_REDIRECT = 308;
    private static final String APPLICATION = "application";
    private static final String JSON = "json";
    private static final String STATUS_CODE = "statusCode";
    private static final String STATUS_MESSAGE = "statusMessage";
    private static final String HEADERS = "headers";
    private static final String HAS_VALUE_KEY = "hasValue";
    private static final String VALUE_KEY = "value";

    private final int statusCode;
    private final String statusMessage;
    private final Headers headers;
    private final byte[] responseBytes;
    private final boolean hasValue;
    private final QueryResults value;
    private final GeaflowRuntimeException exception;

    private JsonResponse(int statusCode, String statusMessage, Headers headers,
                         MediaType contentType, byte[] responseBytes) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBytes = requireNonNull(responseBytes, "responseBytes is null");

        QueryResults value = null;
        GeaflowRuntimeException exception = null;
        if (isJson(contentType)) {
            try {
                Map<String, QueryResults> jsonResult = JSONObject.parseObject(responseBytes,
                    new TypeReference<Map<String, QueryResults>>() {
                    }.getType());
                value = jsonResult.get(HttpAnalyticsServiceHandler.QUERY_RESULT);
            } catch (Exception e) {
                String response = new String(responseBytes, StandardCharsets.UTF_8);
                LOGGER.warn(String.format("Unable to create from JSON response: [%s]", response));
                exception = new GeaflowRuntimeException(String.format("Unable to create from JSON "
                    + "response: [%s]", response), e);
            }
        }
        this.hasValue = (exception == null);
        this.value = value;
        this.exception = exception;
    }

    public static JsonResponse execute(OkHttpClient client, Request request) {
        try (Response response = client.newCall(request).execute()) {
            if ((response.code() == TEMP_REDIRECT) || (response.code() == PERMANENT_REDIRECT)) {
                String location = response.header(LOCATION);
                if (location != null) {
                    request = request.newBuilder().url(location).build();
                    return execute(client, request);
                }
            }

            ResponseBody responseBody = requireNonNull(response.body());
            byte[] responseBytes = responseBody.bytes();
            MediaType contentType = responseBody.contentType();
            return new JsonResponse(response.code(), response.message(), response.headers(), contentType, responseBytes);
        } catch (IOException e) {
            throw new GeaflowRuntimeException("execute http request fail", e);
        }
    }

    private static boolean isJson(MediaType type) {
        return (type != null) && APPLICATION.equals(type.type()) && JSON.equals(type.subtype());
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public Headers getHeaders() {
        return headers;
    }

    public boolean hasValue() {
        return hasValue;
    }

    public QueryResults getValue() {
        if (!hasValue) {
            throw new GeaflowRuntimeException("Response does not contain value", exception);
        }
        return value;
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public GeaflowRuntimeException getException() {
        return exception;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add(STATUS_CODE, statusCode)
            .add(STATUS_MESSAGE, statusMessage)
            .add(HEADERS, headers.toMultimap())
            .add(HAS_VALUE_KEY, hasValue)
            .add(VALUE_KEY, value)
            .omitNullValues()
            .toString();
    }

}
