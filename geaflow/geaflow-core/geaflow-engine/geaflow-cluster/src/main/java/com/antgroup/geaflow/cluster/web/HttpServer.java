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

package com.antgroup.geaflow.cluster.web;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_HTTP_PORT;

import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import com.antgroup.geaflow.cluster.heartbeat.HeartbeatManager;
import com.antgroup.geaflow.cluster.web.handler.ClusterHttpHandler;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);
    private static final String SERVER_NAME = "jetty-server";
    private static final int DEFAULT_ACCEPT_QUEUE_SIZE = 8;

    private final ClusterHttpHandler clusterHandler;
    private final Server server;
    private final int httpPort;
    private final QueuedThreadPool threadPool;
    private final ScheduledExecutorScheduler serverExecutor;

    public HttpServer(Configuration configuration, IClusterManager clusterManager,
                      HeartbeatManager heartbeatManager) {
        this.clusterHandler = new ClusterHttpHandler(clusterManager, heartbeatManager);
        this.httpPort = configuration.getInteger(MASTER_HTTP_PORT);

        threadPool = new QueuedThreadPool();
        threadPool.setDaemon(true);
        threadPool.setName(SERVER_NAME);
        server = new Server(threadPool);

        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks(true);
        errorHandler.setServer(server);
        server.addBean(errorHandler);

        serverExecutor = new ScheduledExecutorScheduler("jetty-scheduler", true);
    }

    public void start() {
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        server.setHandler(contexts);

        //add servlet
        ServletContextHandler contextHandler = new ServletContextHandler(
            ServletContextHandler.SESSIONS);
        contextHandler.addServlet(new ServletHolder(clusterHandler), "/rest/cluster");
        contexts.addHandler(contextHandler);

        try {
            ServerConnector connector = newConnector(server, serverExecutor, null, httpPort);
            connector.setName(SERVER_NAME);
            server.addConnector(connector);

            int minThreads = 1;
            minThreads += connector.getAcceptors() * 2;
            threadPool.setMaxThreads(Math.max(threadPool.getMaxThreads(), minThreads));

            server.start();
            LOGGER.info("Jetty Server started: {}", httpPort);
        } catch (Exception e) {
            LOGGER.error("jetty server failed:", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public void stop() {
        try {
            server.stop();
            if (threadPool.isStarted()) {
                threadPool.stop();
            }
            if (serverExecutor.isStarted()) {
                serverExecutor.stop();
            }
        } catch (Exception e) {
            LOGGER.warn("stop jetty server failed", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    private ServerConnector newConnector(Server server, ScheduledExecutorScheduler serverExecutor,
                                         String hostName, int port) throws Exception {
        ConnectionFactory[] connectionFactories = new ConnectionFactory[]{
            new HttpConnectionFactory()};
        ServerConnector connector = new ServerConnector(server, null, serverExecutor, null, -1, -1,
            connectionFactories);
        connector.setHost(hostName);
        connector.setPort(port);
        connector.start();
        connector.setAcceptQueueSize(Math.min(connector.getAcceptors(), DEFAULT_ACCEPT_QUEUE_SIZE));
        return connector;
    }

}
