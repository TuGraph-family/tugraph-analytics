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

package com.antgroup.geaflow.cluster.heartbeat;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.container.ContainerInfo;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowHeartbeatException;
import com.antgroup.geaflow.common.heartbeat.Heartbeat;
import com.antgroup.geaflow.common.utils.SleepUtils;
import com.antgroup.geaflow.rpc.proto.Master.HeartbeatResponse;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class HeartbeatManagerTest {

    @Mock
    private AbstractClusterManager clusterManager;

    private HeartbeatManager heartbeatManager;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Configuration config = new Configuration();
        config.put(ExecutionConfigKeys.HEARTBEAT_INITIAL_DELAY_MS, "60000");
        config.put(ExecutionConfigKeys.HEARTBEAT_INTERVAL_MS, "60000");
        config.put(ExecutionConfigKeys.HEARTBEAT_TIMEOUT_MS, "500");
        config.put(ExecutionConfigKeys.SUPERVISOR_ENABLE, "true");
        heartbeatManager = new HeartbeatManager(config, clusterManager);
    }

    @AfterMethod
    public void tearDown() {
        heartbeatManager.close();
    }

    @Test
    public void receivedHeartbeat_RegisteredHeartbeat_ReturnsSuccessAndRegistered() {
        Heartbeat heartbeat = new Heartbeat(1);
        when(clusterManager.getContainerInfos()).thenReturn(new HashMap<Integer, ContainerInfo>() {{
            put(1, new ContainerInfo());
        }});

        HeartbeatResponse response = heartbeatManager.receivedHeartbeat(heartbeat);

        assertEquals(true, response.getSuccess());
        assertEquals(true, response.getRegistered());
    }

    @Test
    public void receivedHeartbeat_UnregisteredHeartbeat_ReturnsSuccessAndNotRegistered() {
        Heartbeat heartbeat = new Heartbeat(2);
        when(clusterManager.getContainerInfos()).thenReturn(new HashMap<Integer, ContainerInfo>());

        HeartbeatResponse response = heartbeatManager.receivedHeartbeat(heartbeat);

        assertEquals(true, response.getSuccess());
        assertEquals(false, response.getRegistered());
    }

    @Test
    public void checkHeartBeat_LogsWarningsAndErrors() {
        Map<Integer, String> containerMap = new HashMap<>();
        containerMap.put(1, "container1");
        when(clusterManager.getContainerIds()).thenReturn(containerMap);
        when(clusterManager.getDriverIds()).thenReturn(new HashMap<Integer, String>());

        Heartbeat heartbeat = new Heartbeat(1);
        heartbeatManager.receivedHeartbeat(heartbeat);
        SleepUtils.sleepMilliSecond(600);
        heartbeatManager.checkHeartBeat();

        verify(clusterManager, times(1)).doFailover(eq(1), isA(GeaflowHeartbeatException.class));
    }

    @Test
    public void checkWorkHealth_LogsWarningsAndErrors() {
        Map<Integer, String> containerMap = new HashMap<>();
        containerMap.put(1, "container1");
        when(clusterManager.getContainerIds()).thenReturn(containerMap);
        when(clusterManager.getDriverIds()).thenReturn(new HashMap<Integer, String>());

        heartbeatManager.checkWorkerHealth();
        heartbeatManager.close();

        verify(clusterManager, times(1)).doFailover(eq(1), isA(GeaflowHeartbeatException.class));
    }

    @Test
    public void doFailover_ReportsExceptionAndCallsFailover() {
        int componentId = 1;
        Throwable exception = new RuntimeException("Test exception");

        heartbeatManager.doFailover(componentId, exception);

        verify(clusterManager, times(1)).doFailover(eq(componentId), eq(exception));
    }
}