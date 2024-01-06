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

package com.antgroup.geaflow.cluster.driver;

import com.antgroup.geaflow.cluster.common.AbstractContainer;
import com.antgroup.geaflow.cluster.common.ExecutionIdGenerator;
import com.antgroup.geaflow.cluster.constants.ClusterConstants;
import com.antgroup.geaflow.cluster.exception.ComponentUncaughtExceptionHandler;
import com.antgroup.geaflow.cluster.executor.IPipelineExecutor;
import com.antgroup.geaflow.cluster.executor.PipelineExecutorContext;
import com.antgroup.geaflow.cluster.executor.PipelineExecutorFactory;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.PipelineMasterEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.rpc.ConfigurableServerOption;
import com.antgroup.geaflow.common.utils.PortUtil;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.callback.TaskCallBack;
import com.antgroup.geaflow.pipeline.service.PipelineService;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.baidu.brpc.server.RpcServerOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver process.
 */
public class Driver extends AbstractContainer implements IDriver<IEvent, Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);
    private static final String DRIVER_EXECUTOR = "driver-executor";
    private static final AtomicInteger pipelineTaskIdGenerator = new AtomicInteger(0);

    private DriverEventDispatcher eventDispatcher;
    private DriverContext driverContext;
    private ExecutorService executorService;
    private Map<PipelineService, IPipelineExecutor> pipelineExecutorMap;

    public Driver() {
        this(0);
    }

    public Driver(int rpcPort) {
        super(rpcPort);
    }

    @Override
    public void init(DriverContext driverContext) {
        super.init(driverContext.getId(), ClusterConstants.getDriverName(driverContext.getId()),
            driverContext.getConfig());
        this.driverContext = driverContext;
        this.eventDispatcher = new DriverEventDispatcher();
        this.executorService = Executors.newFixedThreadPool(
            1,
            ThreadUtil.namedThreadFactory(true, DRIVER_EXECUTOR, new ComponentUncaughtExceptionHandler()));
        this.pipelineExecutorMap = new HashMap<>();

        ExecutionIdGenerator.init(id);
        ShuffleManager.getInstance().initShuffleMaster();
        if (driverContext.getPipeline() != null) {
            LOGGER.info("driver {} execute pipeline from recovered context", name);
            executorService.execute(() -> executePipelineInternal(driverContext.getPipeline()));
        }
        registerToMaster();
        registerHAService();
        LOGGER.info("driver {} init finish", name);
    }

    @Override
    protected void startRpcService() {
        RpcServerOptions serverOptions = ConfigurableServerOption.build(configuration);
        this.rpcService = new RpcServiceImpl(PortUtil.getPort(rpcPort), serverOptions);
        this.rpcService.addEndpoint(new DriverEndpoint(this));
        this.rpcService.addEndpoint(new PipelineMasterEndpoint(this));
        this.rpcPort = rpcService.startService();
    }

    @Override
    public Boolean executePipeline(Pipeline pipeline) {
        LOGGER.info("driver {} execute pipeline {}", name, pipeline);
        Future<Boolean> future = executorService.submit(() -> executePipelineInternal(pipeline));
        try {
            return future.get();
        } catch (Throwable e) {
            throw new GeaflowRuntimeException(e);
        }
    }

    public Boolean executePipelineInternal(Pipeline pipeline) {
        try {
            LOGGER.info("start execute pipeline {}", pipeline);
            driverContext.addPipeline(pipeline);
            driverContext.checkpoint(new DriverContext.PipelineCheckpointFunction());

            IPipelineExecutor pipelineExecutor = PipelineExecutorFactory.createPipelineExecutor();
            PipelineExecutorContext executorContext = new PipelineExecutorContext(name, driverContext.getIndex(),
                eventDispatcher, configuration, pipelineTaskIdGenerator);
            pipelineExecutor.init(executorContext);
            pipelineExecutor.register(pipeline.getViewDescMap());

            List<PipelineTask> pipelineTaskList = pipeline.getPipelineTaskList();
            List<TaskCallBack> taskCallBackList = pipeline.getPipelineTaskCallbacks();
            for (int i = 0, size = pipelineTaskList.size(); i < size; i++) {
                if (driverContext.getFinishedPipelineTasks() == null || !driverContext.getFinishedPipelineTasks().contains(i)) {
                    pipelineExecutor.runPipelineTask(pipelineTaskList.get(i),
                        taskCallBackList.get(i));
                    driverContext.addFinishedPipelineTask(i);
                    driverContext.checkpoint(new DriverContext.PipelineTaskCheckpointFunction());
                }
            }

            List<PipelineService> pipelineServices = pipeline.getPipelineServices();
            for (PipelineService pipelineService : pipelineServices) {
                LOGGER.info("execute service");
                pipelineExecutorMap.put(pipelineService, pipelineExecutor);
                pipelineExecutor.startPipelineService(pipelineService);
            }
            LOGGER.info("finish execute pipeline {}", pipeline);
            return true;
        } catch (Throwable e) {
            LOGGER.error("driver exception", e);
            throw e;
        }
    }

    @Override
    public Boolean process(IEvent input) {
        LOGGER.info("{} process event {}", name, input);
        eventDispatcher.dispatch(input);
        return true;
    }

    @Override
    public void close() {
        executorService.shutdownNow();
        for (PipelineService service : pipelineExecutorMap.keySet()) {
            pipelineExecutorMap.get(service).stopPipelineService(service);
        }
        pipelineExecutorMap.clear();

        super.close();
        LOGGER.info("driver {} closed", name);
    }

    @Override
    protected DriverInfo buildComponentInfo() {
        DriverInfo driverInfo = new DriverInfo();
        fillComponentInfo(driverInfo);
        return driverInfo;
    }
}
