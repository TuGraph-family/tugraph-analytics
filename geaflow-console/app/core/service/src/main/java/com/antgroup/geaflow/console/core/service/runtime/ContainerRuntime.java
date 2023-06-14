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

package com.antgroup.geaflow.console.core.service.runtime;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.util.ProcessUtil;
import com.antgroup.geaflow.console.common.util.ThreadUtil;
import com.antgroup.geaflow.console.common.util.ZipUtil;
import com.antgroup.geaflow.console.common.util.exception.GeaflowLogException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.common.util.type.GeaflowTaskStatus;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.model.job.config.GeaflowArgsClass;
import com.antgroup.geaflow.console.core.model.release.GeaflowRelease;
import com.antgroup.geaflow.console.core.model.task.ContainerTaskHandle;
import com.antgroup.geaflow.console.core.model.task.GeaflowTask;
import com.antgroup.geaflow.console.core.model.task.GeaflowTaskHandle;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.file.VersionFileFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ContainerRuntime implements GeaflowRuntime {

    private static final String GEAFLOW_ENGINE_LOG_FILE = "/tmp/logs/task/%s.log";

    @Autowired
    private ContainerTaskParams taskParams;

    @Autowired
    protected VersionFileFactory versionFileFactory;

    @Autowired
    private InstanceService instanceService;

    public static String getLogFilePath(String taskId) {
        return String.format(GEAFLOW_ENGINE_LOG_FILE, TaskParams.getRuntimeTaskName(taskId));
    }

    @Override
    public GeaflowTaskHandle start(GeaflowTask task) {
        GeaflowInstance instance = instanceService.get(task.getRelease().getJob().getInstanceId());
        return doStart(task, taskParams.buildClientArgs(instance, task));
    }

    @Override
    public void stop(GeaflowTask task) {
        try {
            int pid = ((ContainerTaskHandle) task.getHandle()).getPid();
            if (ProcessUtil.existPid(pid)) {
                ProcessUtil.killPid(pid);
            }

            log.info("Stop task {} success, handle={}", task.getId(), JSON.toJSONString(task.getHandle()));

        } catch (Exception e) {
            throw new GeaflowLogException("Stop task {} failed", task.getId(), e);
        }
    }

    @Override
    public GeaflowTaskStatus queryStatus(GeaflowTask task) {
        try {
            return queryStatusWithRetry(task, 5);

        } catch (Exception e) {
            log.error("Query task {} status failed, handle={}", task.getId(), JSON.toJSONString(task.getHandle()), e);
            return GeaflowTaskStatus.FAILED;
        }
    }

    private GeaflowTaskHandle doStart(GeaflowTask task, GeaflowArgsClass geaflowArgs) {
        String runtimeTaskId = geaflowArgs.getSystemArgs().getRuntimeTaskId();
        taskParams.validateRuntimeTaskId(runtimeTaskId);

        try {
            // kill last task process if exists
            GeaflowTaskHandle handle = task.getHandle();
            if (handle != null) {
                int pid = ((ContainerTaskHandle) handle).getPid();
                if (ProcessUtil.existPid(pid)) {
                    ProcessUtil.killPid(pid);
                }
            }

            List<String> classPaths = new ArrayList<>();

            // add version jar
            GeaflowRelease release = task.getRelease();
            String versionName = release.getVersion().getName();
            task.getVersionJars()
                .forEach(jar -> classPaths.add(versionFileFactory.getVersionFile(versionName, jar).getAbsolutePath()));

            // add user jar
            task.getUserJars().forEach(
                jar -> classPaths.add(versionFileFactory.getTaskUserFile(runtimeTaskId, jar).getAbsolutePath()));

            // add release zip
            File releaseFile = versionFileFactory.getTaskReleaseFile(runtimeTaskId, release.getJob().getId(), release);
            ZipUtil.unzip(releaseFile);
            classPaths.add(releaseFile.getParent());

            // start process
            String java = System.getProperty("java.home") + "/bin/java";
            String classPathString = StringUtils.join(classPaths, ":");
            String mainClass = GeaflowTask.CODE_TASK_MAIN_CLASS;
            String args = StringEscapeUtils.escapeJava(JSON.toJSONString(geaflowArgs.build()));
            CommandLine cmd = new CommandLine(java);
            cmd.addArgument("-cp");
            cmd.addArgument(classPathString);
            cmd.addArgument(mainClass);
            cmd.addArgument(args, false);
            String logFile = getLogFilePath(task.getId());
            int pid = ProcessUtil.execAsyncCommand(cmd, 1000, new File(logFile));

            // save handle
            ContainerTaskHandle taskHandle = new ContainerTaskHandle();
            taskHandle.setAppId(runtimeTaskId);
            taskHandle.setClusterType(GeaflowPluginType.CONTAINER);
            taskHandle.setPid(pid);

            log.info("Start task {} success, handle={}", task.getId(), JSON.toJSONString(taskHandle));
            return taskHandle;

        } catch (Exception e) {
            throw new GeaflowLogException("Start task {} failed", task.getId(), e);
        }
    }

    private GeaflowTaskStatus queryStatusWithRetry(GeaflowTask task, int retryTimes) {
        while (retryTimes > 0) {
            try {
                int pid = ((ContainerTaskHandle) task.getHandle()).getPid();
                boolean exist = ProcessUtil.existPid(pid);
                return exist ? GeaflowTaskStatus.RUNNING : GeaflowTaskStatus.FAILED;

            } catch (Exception e) {
                if (--retryTimes == 0) {
                    throw e;
                }

                ThreadUtil.sleepMilliSeconds(500);
            }
        }

        return task.getStatus();
    }

}
