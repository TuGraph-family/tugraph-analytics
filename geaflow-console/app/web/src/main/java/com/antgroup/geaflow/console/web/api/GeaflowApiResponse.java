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

package com.antgroup.geaflow.console.web.api;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.util.NetworkUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowApiResponseCode;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import lombok.Getter;
import org.springframework.http.MediaType;

@Getter
public abstract class GeaflowApiResponse<T> {

    private final boolean success;

    private final String host;

    protected GeaflowApiResponseCode code;

    protected GeaflowApiResponse(boolean success) {
        this.host = NetworkUtil.getHostName();
        this.success = success;
    }

    public static <T> GeaflowApiResponse<T> success(T data) {
        return new SuccessApiResponse<>(data);
    }

    public static <T> GeaflowApiResponse<T> error(Throwable error) {
        return new ErrorApiResponse<>(error);
    }

    public void write(HttpServletResponse response) {
        try {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding(StandardCharsets.UTF_8.toString());
            response.setStatus(GeaflowApiResponseCode.SUCCESS.getHttpCode());
            PrintWriter out = response.getWriter();
            JSON.writeJSONString(out, this);
            out.flush();

        } catch (Exception e) {
            throw new GeaflowException("Write api response failed", e);
        }
    }
}
