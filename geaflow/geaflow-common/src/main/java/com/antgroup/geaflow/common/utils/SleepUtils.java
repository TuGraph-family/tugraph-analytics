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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SleepUtils.class);

    public static void sleepSecond(long second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
            LOGGER.warn("sleep {}s interrupted", second);
            throw new GeaflowRuntimeException(e);
        }
    }

    public static void sleepMilliSecond(long mileSecond) {
        try {
            TimeUnit.MILLISECONDS.sleep(mileSecond);
        } catch (InterruptedException e) {
            LOGGER.warn("sleepMilliSecond {}ms interrupted", mileSecond);
            throw new GeaflowRuntimeException(e);
        }
    }
}
