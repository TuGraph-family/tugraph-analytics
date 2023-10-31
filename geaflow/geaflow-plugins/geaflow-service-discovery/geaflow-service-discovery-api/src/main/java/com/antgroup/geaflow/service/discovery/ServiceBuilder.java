package com.antgroup.geaflow.service.discovery;
import com.antgroup.geaflow.common.config.Configuration;

public interface ServiceBuilder {

    ServiceConsumer buildConsumer(Configuration configuration);

    ServiceProvider buildProvider(Configuration configuration);

    String serviceType();
}
