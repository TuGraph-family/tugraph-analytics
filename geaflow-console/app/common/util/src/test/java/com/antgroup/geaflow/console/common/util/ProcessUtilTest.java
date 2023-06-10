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

import org.testng.Assert;
import org.testng.annotations.Test;


public class ProcessUtilTest {

    @Test
    public void testCommand() throws Exception {
        Assert.assertEquals(ProcessUtil.execute("ls pom.xml"), "pom.xml");
        Assert.assertEquals(ProcessUtil.execute("echo abc"), "abc");

        int pid = ProcessUtil.executeAsync("sleep 123");
        Assert.assertTrue(ProcessUtil.existPid(pid));
        ProcessUtil.killPid(pid);
        Assert.assertFalse(ProcessUtil.existPid(pid));
    }
}
