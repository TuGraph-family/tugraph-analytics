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

package com.antgroup.geaflow.console.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;


public class HTTPUtil {

    public static <T> T getResultData(String result, Type type) {
        return JSON.parseObject(result, type);
    }

    public static <T> T getResultData(String result, TypeReference<T> typeReference) {
        return JSON.parseObject(result, typeReference);
    }

    public static Request get(URIBuilder uri) throws URISyntaxException {
        return Request.Get(uri.build()).addHeader("Accept", "application/json");
    }

    public static Request get(URIBuilder uri, Integer timeout) throws URISyntaxException {
        Request request = get(uri);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static Request get(String url) throws URISyntaxException {
        return Request.Get(url).addHeader("Accept", "application/json");
    }

    public static Request get(String url, Integer timeout) throws URISyntaxException {
        Request request = get(url);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static Request post(String url) {
        return Request.Post(url).addHeader("Accept", "application/json");
    }

    public static Request post(String url, Integer timeout) {
        Request request = post(url);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static Request put(String url) {
        return Request.Put(url).addHeader("Accept", "application/json");
    }

    public static Request put(String url, Integer timeout) {
        Request request = put(url);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static Request put(URIBuilder uri) throws URISyntaxException {
        return Request.Put(uri.build()).addHeader("Accept", "application/json");
    }

    public static Request put(URIBuilder uri, Integer timeout) throws URISyntaxException {
        Request request = put(uri);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static Request delete(URIBuilder uri) throws URISyntaxException {
        return Request.Delete(uri.build()).addHeader("Accept", "application/json");
    }

    public static Request delete(URIBuilder uri, Integer timeout) throws URISyntaxException {
        Request request = delete(uri);
        if (timeout != null) {
            request.connectTimeout(timeout).socketTimeout(timeout);
        }
        return request;
    }

    public static void download(HttpServletResponse response, InputStream inputStream, String fileName)
        throws IOException {
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);

        try (ServletOutputStream output = response.getOutputStream()) {
            IOUtils.copy(inputStream, output, 1024 * 1024 * 8);
            output.flush();
        }
    }
}
