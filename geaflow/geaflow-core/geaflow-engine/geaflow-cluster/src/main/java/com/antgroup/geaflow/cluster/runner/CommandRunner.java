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
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import com.antgroup.geaflow.stats.model.EventLabel;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandRunner.class);

    private int pid;
    private Process process;
    private final String command;
    private final int maxRestarts;
    private final Map<String, String> env;
    private final Configuration configuration;

    public CommandRunner(String command, int maxRestarts, Map<String, String> env,
                         Configuration config) {
        this.command = command;
        this.maxRestarts = maxRestarts;
        this.env = env;
        this.configuration = config;
    }

    public void asyncStart() {
        CompletableFuture.runAsync(() -> {
            try {
                startProcess();
            } catch (Throwable e) {
                LOGGER.error("Start process failed: {}", e.getMessage(), e);
                String errMsg = String.format("Worker process exited: %s", e.getMessage());
                StatsCollectorFactory.init(configuration).getEventCollector()
                    .reportEvent(ExceptionLevel.ERROR, EventLabel.WORKER_PROCESS_EXITED, errMsg);
            }
        });
    }

    public void startProcess() {
        try {
            int restarts = maxRestarts;
            do {
                Process childProcess = doStartProcess(command);
                int code = childProcess.waitFor();
                LOGGER.warn("Child process {} exits with code: {} and alive: {}", pid, code,
                    childProcess.isAlive());
                // 0: success, 137: killed by SIGKILL, 143: killed by SIGTERM
                if (code == 0 || code == 137 || code == 143) {
                    return;
                }
                if (restarts == 0) {
                    String errMsg;
                    if (maxRestarts == 0) {
                        errMsg = String.format("process exits code: %s", code);
                    } else {
                        errMsg = String.format("process exits code: %s, exhausted %s restarts",
                            code, maxRestarts);
                    }
                    throw new GeaflowRuntimeException(errMsg);
                }
                restarts--;
            } while (true);
        } catch (GeaflowRuntimeException e) {
            LOGGER.error("FATAL: start command failed: {}", command, e);
            throw e;
        } catch (Throwable e) {
            LOGGER.error("FATAL: start command failed: {}", command, e);
            throw new GeaflowRuntimeException(e.getMessage(), e);
        }
    }

    private Process doStartProcess(String startCommand) throws IOException {
        LOGGER.info("Start process with command: {}", startCommand);
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", startCommand);
        //pb.redirectInput(Redirect.INHERIT);
        pb.redirectOutput(Redirect.INHERIT);
        if (env != null) {
            pb.environment().putAll(env);
        }
        Process childProcess = pb.start();
        this.process = childProcess;
        this.pid = ProcessUtil.getProcessPid(childProcess);
        LOGGER.info("Process started with pid: {}", pid);
        return childProcess;
    }

    public Process getProcess() {
        return process;
    }

    public int getProcessId() {
        return pid;
    }

}
