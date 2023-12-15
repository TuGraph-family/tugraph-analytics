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

package com.antgroup.geaflow.dashboard.agent;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.dashboard.agent.handler.FlameGraphRestHandler;
import com.antgroup.geaflow.dashboard.agent.handler.LogRestHandler;
import com.antgroup.geaflow.dashboard.agent.handler.ThreadDumpRestHandler;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentWebServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentWebServer.class);

    private static final String AGENT_SERVER_NAME = "agent-jetty-server";

    private static final int DEFAULT_ACCEPT_QUEUE_SIZE = 8;

    private final Server server;

    private final int httpPort;

    private final QueuedThreadPool threadPool;

    private final ScheduledExecutorScheduler serverExecutor;

    private final Object lock = new Object();

    public AgentWebServer(int httpPort, String runtimeLogDirPath,
                          String flameGraphProfilerPath, String agentDir) {
        this.httpPort = httpPort;
        threadPool = new QueuedThreadPool();
        threadPool.setDaemon(true);
        threadPool.setName(AGENT_SERVER_NAME);
        server = new Server(threadPool);

        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setShowStacks(true);
        errorHandler.setServer(server);
        server.addBean(errorHandler);

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new LogRestHandler(runtimeLogDirPath));
        resourceConfig.register(new FlameGraphRestHandler(flameGraphProfilerPath, agentDir));
        resourceConfig.register(new ThreadDumpRestHandler(agentDir));

        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(resourceConfig)), "/rest/*");
        handler.addServlet(new ServletHolder(new DefaultServlet()), "/");

        server.setHandler(handler);

        serverExecutor = new ScheduledExecutorScheduler("jetty-scheduler", true);
    }

    public void start() {
        try {
            ServerConnector connector = newConnector(server, serverExecutor, null, httpPort);
            connector.setName(AGENT_SERVER_NAME);
            server.addConnector(connector);

            int minThreads = 1;
            minThreads += connector.getAcceptors() * 2;
            threadPool.setMaxThreads(Math.max(threadPool.getMaxThreads(), minThreads));

            server.start();
            LOGGER.info("Jetty Server started: {}.", httpPort);
        } catch (Exception e) {
            LOGGER.error("Jetty server failed.", e);
            throw new GeaflowRuntimeException(e);
        }
    }

    public void await() throws InterruptedException {
        LOGGER.info("Wait for agent jetty server stopped.");
        synchronized (lock) {
            lock.wait();
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
            synchronized (lock) {
                lock.notify();
            }
        } catch (Exception e) {
            LOGGER.warn("Stop jetty server failed.", e);
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
