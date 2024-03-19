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

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.llm.GeaflowLLM;
import com.antgroup.geaflow.console.core.model.llm.LLMConfigArgsClass;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Response;

@Slf4j
public abstract class LLMClient {

    public final String call(GeaflowLLM llm, String prompt) {
        try {
            LLMConfigArgsClass config = getConfig(llm, LLMConfigArgsClass.class);
            int retryTimes = config.getRetryTimes();
            int retryInterval = config.getRetryInterval();

            return callWithRetry(llm, prompt, retryTimes, retryInterval);

        } catch (Exception e) {
            log.info("Call language model failed", e);
            throw new GeaflowException("Call language model failed {}", e.getMessage());
        }
    }

    protected <T extends LLMConfigArgsClass> T getConfig(GeaflowLLM llm, Class<T> clazz) {
        GeaflowConfig geaflowConfig = Optional.ofNullable(llm.getArgs()).orElse(new GeaflowConfig());
        T config = geaflowConfig.parse(clazz, true);
        return config;
    }

    private String callWithRetry(GeaflowLLM llm, String prompt, int retryTimes, int retryInterval) {
        for (int i = 0; i < retryTimes; i++) {
            try {
                Response response = sendRequest(llm, prompt);
                return parseResult(response);
            } catch (Exception e) {
                if (i == retryTimes - 1) {
                    throw e;
                }

                try {
                    Thread.sleep(retryInterval * 1000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        return null;
    }

    protected abstract Response sendRequest(GeaflowLLM llm, String prompt);

    protected abstract String parseResult(Response prompt);

    protected OkHttpClient getHttpClient(LLMConfigArgsClass config) {
        long connectTimeout = config.getConnectTimeout();
        long readTimeOut = config.getReadTimeout();
        long writeTimeOut = config.getWriteTimeout();

        return new OkHttpClient().newBuilder()
            .connectTimeout(connectTimeout, TimeUnit.SECONDS)
            .writeTimeout(readTimeOut, TimeUnit.SECONDS)
            .readTimeout(writeTimeOut, TimeUnit.SECONDS)
            .build();
    }
}
