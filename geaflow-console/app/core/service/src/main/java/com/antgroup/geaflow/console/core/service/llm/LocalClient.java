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

package com.antgroup.geaflow.console.core.service.llm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import com.antgroup.geaflow.console.core.model.llm.LocalConfigArgsClass;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class LocalClient extends LLMClient {

    private static final LLMClient INSTANCE = new LocalClient();

    private static final String DEFAULT_N_PREDICT = "128";

    private static final String TEMPLATE = "{"
        + "\"prompt\": \"%s\","
        + "\"n_predict\": %s}";

    private String getJsonString(LocalConfigArgsClass llm, String prompt) {
        Integer predict = llm.getPredict();
        // trim() to replace the last \n 
        return String.format(TEMPLATE, prompt.trim(), predict);
    }

    private LocalClient() {

    }

    public static LLMClient getInstance() {
        return INSTANCE;
    }

    @Override
    protected Response sendRequest(GeaflowLLM llm, String prompt) {
        try {
            LocalConfigArgsClass config = getConfig(llm, LocalConfigArgsClass.class);
            String jsonString = getJsonString(config, prompt);
            OkHttpClient client = getHttpClient(config);
            MediaType type = MediaType.get("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(jsonString, type);

            Request request = new Request.Builder()
                .url(llm.getUrl())
                .addHeader("Content-Type", "application/json")
                .addHeader("Cache-Control", "no-cache")
                .post(body)
                .build();

            Response response = client.newCall(request).execute();
            return response;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String parseResult(Response response) {
        try {
            String res = response.body().string();
            JSONObject jsonObject = JSON.parseObject(res);
            JSONObject error = jsonObject.getJSONObject("error");
            if (error != null) {
                return error.getString("message");
            }
            return jsonObject.getString("content");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
