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
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for retrying to execute.
 */
public class RetryCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryCommand.class);

    public static <T> T run(Callable<T> function, int retryCount) {
        return run(function, retryCount, 0);
    }

    public static <T> T run(Callable<T> function, int retryCount, long retryIntervalMs) {
        while (0 < retryCount) {
            try {
                return function.call();
            } catch (Exception e) {
                retryCount--;
                if (retryCount == 0) {
                    LOGGER.error("Retry failed and reached the maximum retried times.");
                    throw new GeaflowRuntimeException(e);
                }
                LOGGER.warn("Retry failed, will retry {} times with interval {} ms.", retryCount, retryIntervalMs);
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return null;
    }
}
