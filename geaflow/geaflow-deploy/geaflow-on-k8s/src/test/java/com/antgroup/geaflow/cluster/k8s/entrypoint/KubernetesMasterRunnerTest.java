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

package com.antgroup.geaflow.cluster.k8s.entrypoint;

import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class KubernetesMasterRunnerTest extends KubernetesTestBase {

    @Test(expectedExceptions = TimeoutException.class)
    public void testMasterRunner() throws Throwable {
        configuration.put(ExecutionConfigKeys.FO_TIMEOUT_MS, "1000");
        try {
            KubernetesMasterRunner masterRunner = new KubernetesMasterRunner(configuration);
            masterRunner.init();
        } catch (Exception e) {
            throw e.getCause();
        }
    }

}
