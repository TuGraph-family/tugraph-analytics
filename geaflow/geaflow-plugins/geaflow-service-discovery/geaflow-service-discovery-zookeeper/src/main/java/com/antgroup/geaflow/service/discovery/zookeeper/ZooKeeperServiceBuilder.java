package com.antgroup.geaflow.service.discovery.zookeeper;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.service.discovery.ServiceBuilder;
import com.antgroup.geaflow.service.discovery.ServiceConsumer;
import com.antgroup.geaflow.service.discovery.ServiceProvider;

public class ZooKeeperServiceBuilder implements ServiceBuilder {

    private static final String SERVICE_TYPE = "zookeeper";

    @Override
    public ServiceConsumer buildConsumer(Configuration configuration) {
        return new ZooKeeperServiceConsumer(configuration);
    }

    @Override
    public ServiceProvider buildProvider(Configuration configuration) {
        return new ZooKeeperServiceProvider(configuration);
    }

    @Override
    public String serviceType() {
        return SERVICE_TYPE;
    }
}
