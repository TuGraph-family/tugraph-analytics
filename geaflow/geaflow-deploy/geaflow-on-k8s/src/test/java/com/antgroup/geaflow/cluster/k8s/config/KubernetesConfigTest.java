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

package com.antgroup.geaflow.cluster.k8s.config;

import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig.CLIENT_MASTER_URL;
import static com.antgroup.geaflow.cluster.k8s.config.KubernetesConfigKeys.MASTER_URL;

import com.antgroup.geaflow.common.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KubernetesConfigTest {

    @Test
    public void testMasterUrl() {
        Configuration configuration = new Configuration();
        Assert.assertEquals(KubernetesConfig.getClientMasterUrl(configuration),
            MASTER_URL.getDefaultValue());

        configuration.put(CLIENT_MASTER_URL, "");
        Assert.assertEquals(KubernetesConfig.getClientMasterUrl(configuration),
            MASTER_URL.getDefaultValue());

        configuration.put(CLIENT_MASTER_URL, "client");
        Assert.assertEquals(KubernetesConfig.getClientMasterUrl(configuration),
            "client");
    }

    @Test
    public void testJarDownloadPath() {
        Configuration configuration = new Configuration();
        String path = KubernetesConfig.getJarDownloadPath(configuration);
        Assert.assertEquals(path, "/tmp/jar");
    }

}
