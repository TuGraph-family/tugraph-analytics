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

package com.antgroup.geaflow.cluster.k8s.utils;

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.USER_JAR_FILES;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import org.junit.Assert;
import org.testng.annotations.Test;

public class ProgramRunnerTest {

    @Test
    public void testEmptyRun() {
        Configuration configuration = new Configuration();
        ProgramRunner.run(configuration, () -> {
            System.out.println("test run");
        });
    }

    @Test
    public void testRun() {
        Configuration configuration = new Configuration();
        configuration.put(USER_JAR_FILES, "test.jar");
        try {
            ProgramRunner.run(configuration, () -> {
                System.out.println("test run");
            });
        } catch (GeaflowRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("illegal jar"));
        }
    }

}
