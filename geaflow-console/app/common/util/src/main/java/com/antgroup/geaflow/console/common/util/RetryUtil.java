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

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import java.util.concurrent.Callable;

public class RetryUtil {

    public static <T> T exec(Callable<T> function, final int retryCount, long retryIntervalMs) {
        int count = retryCount;
        while (count > 0) {
            try {
                return function.call();

            } catch (Exception e) {
                if (--count == 0) {
                    throw new GeaflowException("exec failed withRetry", e);
                }

                ThreadUtil.sleepMilliSeconds(retryIntervalMs);
            }
        }

        return null;
    }
}
