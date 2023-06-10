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

package com.antgroup.geaflow.cluster.client.callback;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.DSLConfigKeys;
import com.antgroup.geaflow.utils.HttpUtil;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RestClusterStartedCallback implements ClusterStartedCallback {

    private static final String GEAFLOW_TOKEN_KEY = "geaflow-token";

    private final Configuration config;

    private final String callbackUrl;

    private final Map<String, String> headers;

    public RestClusterStartedCallback(Configuration config, String url) {
        this.config = config;
        this.callbackUrl = url;
        this.headers = new HashMap<>();
        this.headers.put(GEAFLOW_TOKEN_KEY, config.getString(DSLConfigKeys.GEAFLOW_DSL_CATALOG_TOKEN_KEY, ""));

    }

    @Override
    public void onSuccess(ClusterMeta clusterInfo) {
        HttpRequest request = new HttpRequest();
        request.setSuccess(true);
        request.setData(clusterInfo);
        HttpUtil.post(callbackUrl, new Gson().toJson(request), headers);
    }

    @Override
    public void onFailure(Throwable e) {
        HttpRequest request = new HttpRequest();
        request.setSuccess(false);
        request.setMessage(e.getMessage());
        HttpUtil.post(callbackUrl, new Gson().toJson(request), headers);
    }

    static class HttpRequest implements Serializable {
        private boolean success;
        private String message;
        private Object data;

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Object getData() {
            return data;
        }

        public void setData(Object data) {
            this.data = data;
        }
    }

}
