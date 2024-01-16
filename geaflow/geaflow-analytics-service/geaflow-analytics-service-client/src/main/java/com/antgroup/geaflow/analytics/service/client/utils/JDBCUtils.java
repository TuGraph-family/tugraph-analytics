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

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public class JDBCUtils {

    public static final String DRIVER_URL_START = "jdbc:tugraph://";

    public static boolean acceptsURL(String url) {
        if (url.startsWith(JDBCUtils.DRIVER_URL_START)) {
            return true;
        }
        throw new GeaflowRuntimeException("Invalid GeaFlow JDBC URL " + url);
    }

}
