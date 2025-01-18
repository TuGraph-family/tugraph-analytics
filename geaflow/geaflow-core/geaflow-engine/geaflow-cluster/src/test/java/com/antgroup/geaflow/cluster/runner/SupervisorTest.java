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

package com.antgroup.geaflow.cluster.runner;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.SleepUtils;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SupervisorTest {
    @Test
    public void test() {
        Configuration configuration = new Configuration();
        Map<String, String> envMap = new HashMap<>();
        String cmd = "sleep 30000";
        Supervisor supervisor = new Supervisor(cmd, configuration,false);
        supervisor.startWorker();
        // wait for process starts.
        while (!supervisor.isWorkerAlive()) {
            SleepUtils.sleepMilliSecond(500);
        }
        supervisor.stopWorker();
        // wait for process exits.
        SleepUtils.sleepSecond(1);
        Assert.assertFalse(supervisor.isWorkerAlive());
        supervisor.stop();
    }
}
