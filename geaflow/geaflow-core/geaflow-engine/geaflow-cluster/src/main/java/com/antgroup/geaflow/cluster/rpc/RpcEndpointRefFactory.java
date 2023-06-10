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

package com.antgroup.geaflow.cluster.rpc;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RPC_ASYNC_THREADS;

import com.antgroup.geaflow.cluster.rpc.impl.ContainerEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.DriverEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.MasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.PipelineMasterEndpointRef;
import com.antgroup.geaflow.cluster.rpc.impl.ResourceManagerEndpointRef;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.utils.ThreadUtil;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RpcEndpointRefFactory implements Serializable {

    private final Map<EndpointRefID, RpcEndpointRef> endpointRefMap;
    private final ExecutorService executorService;

    private static RpcEndpointRefFactory INSTANCE;

    private RpcEndpointRefFactory(Configuration config) {
        this.endpointRefMap = new ConcurrentHashMap<>();
        int threads = config.getInteger(RPC_ASYNC_THREADS);
        this.executorService = new ThreadPoolExecutor(threads, threads, Long.MAX_VALUE,
            TimeUnit.MINUTES, new LinkedBlockingQueue<>(),
            ThreadUtil.namedThreadFactory(true, "rpc-executor"));
    }

    public static synchronized RpcEndpointRefFactory getInstance(Configuration config) {
        if (INSTANCE == null) {
            INSTANCE = new RpcEndpointRefFactory(config);
        }
        return INSTANCE;
    }

    public static synchronized RpcEndpointRefFactory getInstance() {
        return INSTANCE;
    }

    public MasterEndpointRef connectMaster(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.MASTER);
        try {
            return (MasterEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new MasterEndpointRef(host, port, executorService));
        } catch (Throwable t) {
            endpointRefMap.remove(refID);
            throw new RuntimeException("connect master error, host " + host + " port " + port, t);
        }
    }

    public ResourceManagerEndpointRef connectResourceManager(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.RESOURCE_MANAGER);
        try {
            return (ResourceManagerEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new ResourceManagerEndpointRef(host, port, executorService));
        } catch (Throwable t) {
            endpointRefMap.remove(refID);
            throw new RuntimeException("connect rm error, host " + host + " port " + port, t);
        }
    }

    public DriverEndpointRef connectDriver(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.DRIVER);
        try {
            return (DriverEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new DriverEndpointRef(host, port, executorService));
        } catch (Throwable t) {
            endpointRefMap.remove(refID);
            throw new RuntimeException("connect driver error, host " + host + " port " + port, t);
        }
    }

    public PipelineMasterEndpointRef connectPipelineManager(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.PIPELINE_MANAGER);
        try {
            return (PipelineMasterEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new PipelineMasterEndpointRef(host, port, executorService));
        } catch (Throwable t) {
            endpointRefMap.remove(refID);
            throw new RuntimeException("connect pipeline master error, host " + host + " port " + port, t);
        }
    }

    public ContainerEndpointRef connectContainer(String host, int port) {
        EndpointRefID refID = new EndpointRefID(host, port, EndpointType.CONTAINER);
        try {
            return (ContainerEndpointRef) endpointRefMap
                .computeIfAbsent(refID,
                    key -> new ContainerEndpointRef(host, port, executorService));
        } catch (Throwable t) {
            endpointRefMap.remove(refID);
            throw new RuntimeException("connect container error, host " + host + " port " + port, t);
        }
    }

    public void invalidateEndpointCache(String host, int port, EndpointType endpointType) {
        EndpointRefID refID = new EndpointRefID(host, port, endpointType);
        endpointRefMap.remove(refID);
    }

    enum EndpointType {
        MASTER, RESOURCE_MANAGER, DRIVER, PIPELINE_MANAGER, CONTAINER
    }

    public static class EndpointRefID {

        private String host;
        private int port;
        private EndpointType endpointType;

        public EndpointRefID(String host, int port, EndpointType endpointType) {
            this.host = host;
            this.port = port;
            this.endpointType = endpointType;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public EndpointType getEndpointType() {
            return endpointType;
        }

        public void setEndpointType(EndpointType endpointType) {
            this.endpointType = endpointType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EndpointRefID that = (EndpointRefID) o;
            return port == that.port && host.equals(that.host) && endpointType == that.endpointType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port, endpointType);
        }

        @Override
        public String toString() {
            return "EndpointRefID{" + "host='" + host + '\'' + ", port=" + port + ", endpointType="
                + endpointType + '}';
        }
    }

}
