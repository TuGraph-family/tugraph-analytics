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

import com.antgroup.geaflow.cluster.k8s.config.KubernetesConfig;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.CustomClassLoader;
import java.net.URL;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgramRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProgramRunner.class);

    public static void run(Configuration config, Runnable runnable) {
        runnable.run();
    }

    private static ClassLoader getClassloader(Configuration config) {
        String userJarFiles = config.getString(USER_JAR_FILES);
        if (StringUtils.isNotEmpty(userJarFiles)) {
            String jarPath = KubernetesConfig.getJarDownloadPath(config);
            List<URL> jarUrls = DownloadHelper.downloadJarPackage(jarPath, userJarFiles);
            LOGGER.info("created new classloader with URLs:{}", jarUrls);
            return new CustomClassLoader(jarUrls.toArray(new URL[0]),
                ProgramRunner.class.getClassLoader());
        }
        return null;
    }

}
