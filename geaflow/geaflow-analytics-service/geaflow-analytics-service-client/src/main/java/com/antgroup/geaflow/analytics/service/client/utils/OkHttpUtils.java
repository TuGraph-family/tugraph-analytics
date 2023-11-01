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

package com.antgroup.geaflow.analytics.service.client.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.util.Objects.requireNonNull;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.google.common.base.CharMatcher;
import java.util.concurrent.TimeUnit;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;

public class OkHttpUtils {

    public static Interceptor basicAuth(String user, String password) {
        requireNonNull(user, "client user is null");
        requireNonNull(password, "client password is null");
        if (user.contains(":")) {
            throw new GeaflowRuntimeException("Illegal character ':' found in username");
        }
        String credential = Credentials.basic(user, password);
        return chain -> chain.proceed(chain.request().newBuilder()
            .header(AUTHORIZATION, credential)
            .build());
    }

    public static Interceptor tokenAuth(String accessToken) {
        requireNonNull(accessToken, "accessToken is null");
        checkArgument(CharMatcher.inRange((char) 33, (char) 126).matchesAllOf(accessToken));
        return chain -> chain.proceed(chain.request().newBuilder()
            .addHeader(AUTHORIZATION, "Bearer " + accessToken)
            .build());
    }

    public static void setupTimeouts(OkHttpClient.Builder clientBuilder, int timeout, TimeUnit unit) {
        clientBuilder.connectTimeout(timeout, unit)
            .readTimeout(timeout, unit)
            .writeTimeout(timeout, unit);
    }

}
