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

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_DISPATCH_THREADS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.REPORTER_LIST;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;

import com.antgroup.geaflow.cluster.exception.ExceptionCollectService;
import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
import com.antgroup.geaflow.cluster.protocol.OpenContainerEvent;
import com.antgroup.geaflow.cluster.task.ITaskContext;
import com.antgroup.geaflow.cluster.util.SystemExitSignalCatcher;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ReflectionUtil;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.ha.service.HAServiceFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContainerTest {

    private static AtomicBoolean eventExecuted = new AtomicBoolean(false);
    private static AtomicBoolean hasException = new AtomicBoolean(false);
    private static SecurityManager securityManager;

    @BeforeClass
    public void before() {
        securityManager = System.getSecurityManager();
        System.setSecurityManager(new SystemExitSignalCatcher(hasException));
    }

    @AfterClass
    public void after() {
        System.setSecurityManager(securityManager);
    }

    @BeforeMethod
    public void beforeMethod() {
        hasException.set(false);
    }

    @Test
    public void testProcessEventHandleException() throws Exception{

        eventExecuted.set(false);
        Container container = new Container();
        Map<String, String> config = new HashMap<>();
        config.put(CLUSTER_ID.getKey(), "0");
        config.put(REPORTER_LIST.getKey(), "slf4j");
        config.put(RUN_LOCAL_MODE.getKey(), "true");
        config.put(CONTAINER_DISPATCH_THREADS.getKey(), "1");
        Configuration configuration = new Configuration(config);
        ReflectionUtil.setField(container, "name", "test");
        ReflectionUtil.setField(container, "configuration", configuration);
        ReflectionUtil.setField(container, "haService", HAServiceFactory.getService(configuration));
        ReflectionUtil.setField(container, "containerContext", new ContainerContext(0, configuration));
        ReflectionUtil.setField(container, "exceptionCollectService", new ExceptionCollectService());
        container.open(new OpenContainerEvent(1));
        container.process(new TestCreateTaskEvent());
        container.process(new ExceptionCommandEvent());

        waitTestResult();
        Assert.assertTrue(hasException.get());
        container.close();
    }

    @Test
    public void testProcessMultiEventHandleException() throws Exception {

        eventExecuted.set(false);
        Container container = new Container();
        Map<String, String> config = new HashMap<>();
        config.put(CLUSTER_ID.getKey(), "0");
        config.put(REPORTER_LIST.getKey(), "slf4j");
        config.put(RUN_LOCAL_MODE.getKey(), "true");
        config.put(CONTAINER_DISPATCH_THREADS.getKey(), "1");
        Configuration configuration = new Configuration(config);
        ReflectionUtil.setField(container, "name", "test");
        ReflectionUtil.setField(container, "configuration", configuration);
        ReflectionUtil.setField(container, "haService", HAServiceFactory.getService(configuration));
        ReflectionUtil.setField(container, "containerContext", new ContainerContext(0, configuration));
        ReflectionUtil.setField(container, "exceptionCollectService", new ExceptionCollectService());
        container.open(new OpenContainerEvent(1));
        container.process(new TestCreateTaskEvent());
        container.process(new ExceptionCommandEvent());

        waitTestResult();
        Assert.assertTrue(hasException.get());
        container.close();
    }

    private void waitTestResult() {
        int retry = 10;
        while (!eventExecuted.compareAndSet(true, false) && retry > 0) {
            SleepUtils.sleepMilliSecond(100);
            retry --;
        }
        retry = 10;
        while (!hasException.get() && retry > 0) {
            SleepUtils.sleepMilliSecond(100);
            retry --;
        }
    }

    static class ExceptionCommandEvent implements IExecutableCommand {

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public EventType getEventType() {
            return EventType.INIT_CYCLE;
        }

        @Override
        public void execute(ITaskContext taskContext) {
            eventExecuted.set(true);
            throw new RuntimeException("fatal error");
        }

        @Override
        public void interrupt() {

        }
    }

    static class TestCreateTaskEvent implements ICommand {

        @Override
        public int getWorkerId() {
            return 0;
        }

        @Override
        public EventType getEventType() {
            return EventType.CREATE_TASK;
        }
    }
}
