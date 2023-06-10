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

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class KubernetesClientRunnerTest extends KubernetesTestBase {

    @Test
    public void testClientRunner() {
        configuration.put(ExecutionConfigKeys.REGISTER_TIMEOUT, "2");
        configuration.put(KubernetesConfigKeys.USER_MAIN_CLASS,
            KubernetesMockRunner.class.getCanonicalName());
        String clusterArgs = JSON.toJSONString(configuration.getConfigMap());
        KubernetesClientRunner clientRunner = new KubernetesClientRunner(configuration);
        clientRunner.run(clusterArgs);
    }

}
