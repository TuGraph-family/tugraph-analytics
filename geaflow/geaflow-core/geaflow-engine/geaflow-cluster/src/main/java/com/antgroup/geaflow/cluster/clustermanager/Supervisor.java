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

package com.antgroup.geaflow.cluster.clustermanager;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.EXIT_WAIT_SECONDS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_MAX_RESTARTS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.SUPERVISOR_RPC_PORT;

import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.rpc.impl.SupervisorEndpoint;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.ConfigurableServerOption;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import com.baidu.brpc.server.RpcServerOptions;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Supervisor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Supervisor.class);

    private final RpcServiceImpl rpcService;
    private final String startCommand;
    private final List<String> serviceCommands;
    private final AtomicInteger maxRestarts;
    private final Configuration configuration;
    private Process process;

    public Supervisor(String startCommand, List<String> serviceCommands,
                      Configuration configuration, boolean autoRestart) {
        this.startCommand = startCommand;
        this.serviceCommands = serviceCommands;
        this.configuration = configuration;
        int retryTimes = autoRestart ? configuration.getInteger(FO_MAX_RESTARTS) : 0;
        this.maxRestarts = new AtomicInteger(retryTimes);

        RpcServerOptions serverOptions = getServerOptions(configuration);
        int port = configuration.getInteger(SUPERVISOR_RPC_PORT);
        this.rpcService = new RpcServiceImpl(port, serverOptions);
        this.rpcService.addEndpoint(new SupervisorEndpoint(this));
        this.rpcService.startService();
    }

    private RpcServerOptions getServerOptions(Configuration configuration) {
        RpcServerOptions serverOptions = ConfigurableServerOption.build(configuration);
        serverOptions.setGlobalThreadPoolSharing(false);
        serverOptions.setIoThreadNum(1);
        serverOptions.setWorkThreadNum(2);
        return serverOptions;
    }

    public void start() {
        LOGGER.info("Start supervisor with maxRestarts:{}", maxRestarts);
        if (!serviceCommands.isEmpty()) {
            for (String command : serviceCommands) {
                asyncStartProcess(command, Integer.MAX_VALUE);
            }
        }
        startProcess(startCommand, maxRestarts.get(), true);
    }

    public void restartWorkerProcess(int pid) {
        LOGGER.info("Restart worker process: {}", pid);
        try {
            doStopProcess(pid);
            doStartProcess(startCommand, true);
        } catch (IOException e) {
            LOGGER.error("Restart process failed", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public boolean isWorkerProcessAlive() {
        if (maxRestarts.get() > 0 || process != null && process.isAlive()) {
            return true;
        }
        LOGGER.warn("Worker process is dead.");
        return false;
    }

    private synchronized Process doStartProcess(String startCommand, boolean isMainProcess) throws IOException {
        if (isMainProcess && process != null && process.isAlive()) {
            LOGGER.warn("Process is alive before restarting!");
        }
        LOGGER.info("Start process with command: {}", startCommand);
        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", startCommand);
        Process process = pb.start();
        if (isMainProcess) {
            this.process = process;
        }
        LOGGER.info("Process started with pid: {}", ProcessUtil.getProcessPid(process));
        return process;
    }

    private synchronized void doStopProcess(int pid) {
        Preconditions.checkArgument(pid > 0, "pid should be larger than 0");
        LOGGER.info("Kill process: {}", pid);
        ProcessUtil.killProcess(pid);

        if (process != null && process.isAlive()) {
            int ppid = ProcessUtil.getProcessPid(process);
            if (ppid <= 0) {
                LOGGER.warn("NOT found live process {}", process);
                return;
            }
            if (pid != ppid) {
                LOGGER.info("Kill parent process: {}", ppid);
                process.destroy();
            }
        }
    }

    private void startProcess(String command, int restarts, boolean isMainProcess) {
        try {
            do {
                Process process = doStartProcess(command, isMainProcess);
                int code = process.waitFor();
                if (code != 0) {
                    LOGGER.warn("Child process exits with code: {} command: {}", code, command);
                }
                restarts--;
            } while (restarts >= 0);
        } catch (Exception e) {
            StatsCollectorFactory.init(configuration).getExceptionCollector()
                .reportException(ExceptionLevel.FATAL, e);
            SleepUtils.sleepSecond(EXIT_WAIT_SECONDS);
            if (e instanceof GeaflowRuntimeException) {
                throw (GeaflowRuntimeException) e;
            }
            throw new GeaflowRuntimeException(e);
        }
    }

    private void asyncStartProcess(String command, int restarts) {
        CompletableFuture.runAsync(() -> {
            startProcess(command, restarts, false);
        });
    }

    public void waitForTermination() {
        if (rpcService != null) {
            rpcService.waitTermination();
        }
    }

}
