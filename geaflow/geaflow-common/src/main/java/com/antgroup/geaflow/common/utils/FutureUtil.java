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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FutureUtil {

    public static <T> List<T> wait(List<Future<T>> futureList) {
        return wait(futureList, 0);
    }

    public static <T> List<T> wait(List<Future<T>> futureList, int timeoutMs) {
        List<T> result = new ArrayList<>();
        for (Future<T> future : futureList) {
            try {
                if (timeoutMs > 0) {
                    result.add(future.get(timeoutMs, TimeUnit.MILLISECONDS));
                } else {
                    result.add(future.get());
                }
            } catch (Exception e) {
                throw new GeaflowRuntimeException(e);
            }
        }
        return result;
    }
}
