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
import org.testng.Assert;
import org.testng.annotations.Test;

public class RetryCommandTest {

    @Test(expectedExceptions = GeaflowRuntimeException.class)
    public void testFail() {
        final int time = 0;
        RetryCommand.run(() -> 10 / time > 0, 3, 1);
    }

    @Test
    public void testRun() {
        Object result = RetryCommand.run(() -> {
            System.out.println("hello");
            return "null";
        }, 1, 100);
        Assert.assertEquals(result, "null");
    }

    @Test
    public void testException() {
        Object result = null;
        try {
            RetryCommand.run(() -> {
                throw new RuntimeException("exception");
            }, 1, 100);
        } catch (Throwable e) {
            result = e;
        }
        Assert.assertTrue(result instanceof RuntimeException);
    }
}
