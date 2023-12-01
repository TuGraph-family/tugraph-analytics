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

package com.antgroup.geaflow.analytics.service.client;

import static com.antgroup.geaflow.analytics.service.config.keys.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM;

import com.antgroup.geaflow.analytics.service.config.keys.AnalyticsClientConfigKeys;
import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.common.mode.JobMode;
import com.antgroup.geaflow.service.discovery.zookeeper.ZooKeeperConfigKeys;
import com.google.common.base.Preconditions;

public class AnalyticsClientBuilder {

    private final Configuration configuration = new Configuration();

    private String host;

    private int port;

    private String user;

    private boolean needAuth;

    private int queryRetryNum;

    private String zkBaseNode;

    private String zkQuorumServer;

    private boolean initChannelPools;

    public AnalyticsClientBuilder() {
    }

    public AnalyticsClientBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public AnalyticsClientBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public AnalyticsClientBuilder withInitChannelPools(boolean initChannelPools) {
        this.initChannelPools = initChannelPools;
        return this;
    }

    public AnalyticsClientBuilder withNeedAuth(boolean needAuth) {
        this.needAuth = needAuth;
        return this;
    }

    public AnalyticsClientBuilder withConfiguration(Configuration configuration) {
        this.configuration.putAll(configuration.getConfigMap());
        return this;
    }

    public AnalyticsClientBuilder withUser(String user) {
        this.user = user;
        return this;
    }

    public AnalyticsClientBuilder withAnalyticsZkNode(String zkBaseNode) {
        this.configuration.put(ZooKeeperConfigKeys.ZOOKEEPER_BASE_NODE.getKey(), zkBaseNode);
        this.zkBaseNode = zkBaseNode;
        return this;
    }

    public AnalyticsClientBuilder withAnalyticsZkQuorumServers(String zkQuorumServer) {
        this.configuration.put(ZooKeeperConfigKeys.ZOOKEEPER_QUORUM_SERVERS.getKey(), zkQuorumServer);
        this.zkQuorumServer = zkQuorumServer;
        return this;
    }

    public AnalyticsClientBuilder withTimeoutMs(int timeoutMs) {
        this.configuration.put(AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_TIMEOUT_MS, String.valueOf(timeoutMs));
        return this;
    }

    public AnalyticsClientBuilder withRetryNum(int retryNum) {
        this.configuration.put(ANALYTICS_CLIENT_CONNECT_RETRY_NUM, String.valueOf(retryNum));
        this.queryRetryNum = retryNum;
        return this;
    }

    public AnalyticsClient build() {
        if (host == null) {
            checkAnalyticsClientConfig(configuration);
        }
        return new AnalyticsClient(this);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public boolean isNeedAuth() {
        return needAuth;
    }

    public int getQueryRetryNum() {
        return queryRetryNum;
    }

    public String getZkBaseNode() {
        return zkBaseNode;
    }

    public String getZkQuorumServer() {
        return zkQuorumServer;
    }

    public boolean isInitChannelPools() {
        return initChannelPools;
    }

    public static void checkAnalyticsClientConfig(Configuration config) {
        // Check job mode.
        checkAnalyticsClientJobMode(config);
        configIsExist(config, ZooKeeperConfigKeys.ZOOKEEPER_BASE_NODE);
        configIsExist(config, ZooKeeperConfigKeys.ZOOKEEPER_QUORUM_SERVERS);
    }

    private static void checkAnalyticsClientJobMode(Configuration config) {
        if (config.contains(ExecutionConfigKeys.JOB_MODE)) {
            JobMode jobMode = JobMode.getJobMode(config);
            Preconditions.checkArgument(JobMode.OLAP_SERVICE.equals(jobMode), "analytics job mode must set OLAP_SERVICE");
            return;
        }
        throw new GeaflowRuntimeException("analytics client config miss: " + ExecutionConfigKeys.JOB_MODE.getKey());
    }

    private static void configIsExist(Configuration config, ConfigKey configKey) {
        Preconditions.checkArgument(
            config.contains(configKey) && !config.getConfigMap().get(configKey.getKey()).isEmpty(),
            "client missing config: " + configKey.getKey() + ", description: "
                + configKey.getDescription());
    }

}