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

package com.antgroup.geaflow.cluster.ray.utils;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_WORK_PATH;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;

import com.antgroup.geaflow.cluster.config.ClusterConfig;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.utils.math.MathUtil;
import io.ray.api.Ray;
import io.ray.runtime.config.RunMode;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaySystemFunc implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaySystemFunc.class);


    private static final Object LOCK = new Object();
    private static String appPath;

    public static boolean isRestarted() {
        return Ray.getRuntimeContext().wasCurrentActorRestarted();
    }

    public static boolean isLocalMode() {
        return Ray.getRuntimeContext().isLocalMode();
    }

    public static String getWorkPath() {
        if (appPath != null) {
            return appPath;
        }
        synchronized (LOCK) {
            if (Ray.getRuntimeContext().isLocalMode()) {
                appPath = "/tmp/" + System.currentTimeMillis();
                try {
                    FileUtils.forceMkdir(new File(appPath));
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            } else {
                appPath = Ray.getRuntimeContext().getCurrentRuntimeEnv()
                    .get(RayConfig.RAY_JOB_WORKING_DIR, String.class);
            }
        }
        return appPath;
    }

    public static void initRayEnv(ClusterConfig clusterConfig) {
        LOGGER.info("clusterConfig:{}", clusterConfig);
        Configuration config = clusterConfig.getConfig();

        Map<String, String> systemProperties = new LinkedHashMap<>();
        boolean isLocal = config.getBoolean(RUN_LOCAL_MODE);
        if (isLocal) {
            systemProperties.put(RayConfig.RAY_RUN_MODE, RunMode.LOCAL.name());
        } else {
            systemProperties.put(RayConfig.RAY_RUN_MODE, RunMode.CLUSTER.name());
        }

        // Sets how many actor threads can be started by a jvm process.
        int containerMemoryMb = clusterConfig.getContainerMemoryMB();
        // The amount of memory used by a jvm process, both in and out of the heap, must be a factor of 50.
        systemProperties.put(RayConfig.RAY_JOB_JAVA_WORKER_PROCESS_DEFAULT_MEMORY_MB,
            String.valueOf(MathUtil.multiplesOf50(containerMemoryMb)));

        // To set how many jvm processes are started there, geaflow defaults to 0.
        systemProperties.put(RayConfig.RAY_JOB_NUM_INITIAL_JAVA_WORKER_PROCESS, "0");

        // Set all resources required for this job
        // Includes all node memory, as well as additional master and driver memory for the engine,
        // and reserves additional memory for normal tasks
        // totalMemory can be set to a larger value. ray will report an error if more memory is used than VC.
        int totalDriverMemoryMb = clusterConfig.getDriverMemoryMB() * clusterConfig.getDriverNum();

        int totalMasterMemoryMb = clusterConfig.getMasterMemoryMB();

        int totalMemoryMb =
            containerMemoryMb * clusterConfig.getContainerNum() + totalDriverMemoryMb
                + totalMasterMemoryMb + RayConfig.CLUSTER_RESERVED_MEMORY_MB;
        systemProperties.put(RayConfig.RAY_JOB_TOTAL_MEMORY_MB,
            String.valueOf(MathUtil.multiplesOf50(totalMemoryMb)));

        // Set the jvm process heap memory ratio
        // If clusterConfig is empty, set the default Xmx percentage of total memory
        // Otherwise, do not set this ratio, but set each jvm parameter separately in setting jvm parameters later
        systemProperties.put(RayConfig.RAY_JOB_JAVA_HEAP_FRACTION, "0.8");

        systemProperties.put(RayConfig.RAY_TASK_RETURN_TASK_EXCEPTION, Boolean.FALSE.toString());


        // Set the JVM parameters below
        int optionIndex = 0;
        while (System.getProperty(
            String.format("%s.%d", RayConfig.RAY_JOB_JVM_OPTIONS_PREFIX, optionIndex)) != null) {
            optionIndex++;
        }

        // jvm parameter
        if (clusterConfig.getContainerJvmOptions() != null) {
            for (String option : clusterConfig.getContainerJvmOptions().getJvmOptions()) {
                systemProperties.put(String.format("%s.%d", RayConfig.RAY_JOB_JVM_OPTIONS_PREFIX, optionIndex++),
                    option);
            }
        }

        // Set the user log file configuration as follows
        systemProperties
            .put(String.format("%s.%d", RayConfig.RAY_JOB_JVM_OPTIONS_PREFIX, optionIndex++), String
                .format("-D%s=%s", RayConfig.RAY_CUSTOM_LOGGER0_NAME,
                    RayConfig.CUSTOM_LOGGER_NAME));

        systemProperties
            .put(String.format("%s.%d", RayConfig.RAY_JOB_JVM_OPTIONS_PREFIX, optionIndex++), String
                .format("-D%s=%s", RayConfig.RAY_CUSTOM_LOGGER0_FILE_NAME,
                    RayConfig.CUSTOM_LOGGER_FILE_NAME));

        systemProperties
            .put(String.format("%s.%d", RayConfig.RAY_JOB_JVM_OPTIONS_PREFIX, optionIndex++), String
                .format("-D%s=%s", RayConfig.RAY_CUSTOM_LOGGER0_PATTERN,
                    RayConfig.CUSTOM_LOGGER_PATTERN));

        LOGGER.info("set system property: {}", systemProperties);

        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue());
        }

        // Set working dir.
        System.setProperty(
            String.format("%s.%s", RayConfig.RAY_JOB_RUNTIME_ENV, RayConfig.RAY_JOB_WORKING_DIR),
            config.getString(JOB_WORK_PATH));
        Ray.init();
    }
}
