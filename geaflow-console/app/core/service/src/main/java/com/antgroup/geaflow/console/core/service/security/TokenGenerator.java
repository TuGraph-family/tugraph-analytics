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

package com.antgroup.geaflow.console.core.service.security;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class TokenGenerator {

    private static final String TASK_TOKEN_PREFIX = "TASK-";

    public static boolean isTaskToken(String token) {
        return StringUtils.startsWith(token, TASK_TOKEN_PREFIX);
    }

    public String nextToken() {
        return RandomStringUtils.randomAlphanumeric(32);
    }

    public String nextTaskToken() {
        return TASK_TOKEN_PREFIX + nextToken();
    }

}
