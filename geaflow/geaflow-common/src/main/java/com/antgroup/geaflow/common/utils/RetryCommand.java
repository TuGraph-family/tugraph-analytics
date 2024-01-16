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

package com.antgroup.geaflow.common.utils;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Random;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for retrying to execute.
 */
public class RetryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryCommand.class);
    private static final Random RANDOM = new Random();

    public static <T> T run(Callable<T> function, int retryCount) {
        return run(function, retryCount, 0);
    }

    public static <T> T run(Callable<T> function, int retryCount, long retryIntervalMs) {
        return run(function, null, retryCount, retryIntervalMs);
    }

    public static <T> T run(Callable<T> function, Callable retryFunction, int retryCount,
                            long retryIntervalMs) {
        return run(function, retryFunction, retryCount, retryIntervalMs, false);
    }

    public static <T> T run(Callable<T> function, Callable retryFunction, final int retryCount,
                            long retryIntervalMs, boolean needRandom) {
        int i = retryCount;
        while (0 < i) {
            try {
                return function.call();
            } catch (Exception e) {
                i--;

                if (i == 0) {
                    LOGGER.error("Retry failed and reached the maximum retried times.", e);
                    throw new GeaflowRuntimeException(e);
                }

                try {
                    long sleepTime = needRandom ? retryIntervalMs * (RANDOM.nextInt(retryCount) + 1)
                                                : retryIntervalMs;
                    LOGGER.warn("Retry failed, will retry {} times with interval {} ms", i,
                        sleepTime);
                    Thread.sleep(sleepTime);
                    if (retryFunction != null) {
                        retryFunction.call();
                    }
                } catch (Exception e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return null;
    }
}