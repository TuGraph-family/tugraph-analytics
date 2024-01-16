package com.antgroup.geaflow.analytics.service.client;

import static com.antgroup.geaflow.analytics.service.client.AnalyticsClient.checkAnalyticsClientConfig;
import static com.antgroup.geaflow.analytics.service.config.AnalyticsClientConfigKeys.ANALYTICS_CLIENT_CONNECT_RETRY_NUM;

import com.antgroup.geaflow.analytics.service.config.AnalyticsClientConfigKeys;
import com.antgroup.geaflow.common.config.Configuration;

public class AnalyticsClientBuilder {
    private final Configuration configuration = new Configuration();
    private String host;
    private int port;
    private String user;
    private int queryRetryNum;
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

    public AnalyticsClientBuilder withConfiguration(Configuration configuration) {
        this.configuration.putAll(configuration.getConfigMap());
        return this;
    }

    public AnalyticsClientBuilder withUser(String user) {
        this.user = user;
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

    public int getQueryRetryNum() {
        return queryRetryNum;
    }

    public boolean enableInitChannelPools() {
        return initChannelPools;
    }

    public AnalyticsClient build() {
        if (host == null) {
            checkAnalyticsClientConfig(configuration);
        }
        return new AnalyticsClient(this);
    }

}
