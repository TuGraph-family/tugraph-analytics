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

package com.antgroup.geaflow.kubernetes.operator.core.util;

import com.antgroup.geaflow.kubernetes.operator.core.model.exception.GeaflowRuntimeException;
import java.net.InetAddress;

public class CommonUtil {

    private static String HOST_NAME;

    public static String getHostName() {
        if (HOST_NAME != null) {
            return HOST_NAME;
        }

        try {
            return HOST_NAME = InetAddress.getLocalHost().getHostName();

        } catch (Exception e) {
            throw new GeaflowRuntimeException("Init local hostname failed", e);
        }
    }

}
