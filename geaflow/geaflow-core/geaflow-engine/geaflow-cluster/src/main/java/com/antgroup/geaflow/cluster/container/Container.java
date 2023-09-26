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

package com.antgroup.geaflow.cluster.container;

import com.antgroup.geaflow.cluster.collector.EmitterService;
import com.antgroup.geaflow.cluster.common.AbstractContainer;
import com.antgroup.geaflow.cluster.fetcher.FetcherService;
import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.protocol.IEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.protocol.OpenContainerResponseEvent;
import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpoint;
import com.antgroup.geaflow.cluster.rpc.impl.RpcServiceImpl;
import com.antgroup.geaflow.cluster.task.service.TaskService;
import com.antgroup.geaflow.cluster.worker.Dispatcher;
import com.antgroup.geaflow.cluster.worker.DispatcherService;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Container extends AbstractContainer implements IContainer<IEvent, IEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

    private static final String CONTAINER_NAME_PREFIX = "container-";

    private ContainerContext containerContext;
    private Dispatcher dispatcher;
    protected FetcherService fetcherService;
    protected EmitterService emitterService;
    protected TaskService workerService;
    protected DispatcherService dispatcherService;

    public Container() {
        this(0);
    }

    public Container(int rpcPort) {
        super(rpcPort);
    }

    @Override
    public void init(ContainerContext containerContext) {
        try {
            this.containerContext = containerContext;
            super.init(containerContext.getId(), CONTAINER_NAME_PREFIX, containerContext.getConfig());
            registerToMaster();
            LOGGER.info("container {} init finish", name);
        } catch (Throwable t) {
            LOGGER.error("init container err", t);
            throw new GeaflowRuntimeException(t);
        }
    }

    @Override
    protected void startRpcService() {
        this.rpcService = new RpcServiceImpl(rpcPort, configuration);
        this.rpcService.addEndpoint(new ContainerEndpoint(this));
        this.rpcPort = rpcService.startService();
    }

    public OpenContainerResponseEvent open(OpenContainerEvent event) {
        try {
            int num = event.getExecutorNum();
            Preconditions.checkArgument(num > 0, "worker num should > 0");
            LOGGER.info("open container {} with {} executors", name, num);

            this.fetcherService = new FetcherService(num, configuration);
            this.emitterService = new EmitterService(num, configuration);
            this.workerService = new TaskService(id, num,
                configuration, metricGroup, fetcherService, emitterService);
            this.dispatcher = new Dispatcher(workerService);
            this.dispatcherService = new DispatcherService(dispatcher);

            // start task service
            this.fetcherService.start();
            this.emitterService.start();
            this.workerService.start();
            this.dispatcherService.start();

            if (containerContext.getReliableEvents() != null) {
                for (IEvent reliableEvent : containerContext.getReliableEvents()) {
                    LOGGER.info("{} replay event {}", name, reliableEvent);
                    this.dispatcher.add((ICommand) reliableEvent);
                }
            }
            registerHAService();
            return new OpenContainerResponseEvent(id, 0);
        } catch (Throwable throwable) {
            LOGGER.error("{} open error", name, throwable);
            throw throwable;
        }
    }

    @Override
    public IEvent process(IEvent input) {
        LOGGER.info("{} process event {}", name, input);
        try {
            this.containerContext.addEvent(input);
            this.containerContext.checkpoint(new ContainerContext.EventCheckpointFunction());
            this.dispatcher.add((ICommand) input);
            return null;
        } catch (Throwable throwable) {
            LOGGER.error("{} process error", name, throwable);
            throw throwable;
        }
    }

    @Override
    public void close() {
        super.close();
        if (fetcherService != null) {
            fetcherService.shutdown();
        }
        if (workerService != null) {
            workerService.shutdown();
        }
        if (dispatcherService != null) {
            dispatcherService.shutdown();
        }
        if (emitterService != null) {
            emitterService.shutdown();
        }
        LOGGER.info("container {} closed", name);
    }

    protected ContainerInfo buildComponentInfo() {
        ContainerInfo containerInfo = new ContainerInfo();
        containerInfo.setId(id);
        containerInfo.setName(name);
        containerInfo.setPid(ProcessUtil.getProcessId());
        containerInfo.setHost(ProcessUtil.getHostIp());
        containerInfo.setRpcPort(rpcPort);
        containerInfo.setShufflePort(ShuffleManager.getInstance().getShufflePort());
        return containerInfo;
    }
}
