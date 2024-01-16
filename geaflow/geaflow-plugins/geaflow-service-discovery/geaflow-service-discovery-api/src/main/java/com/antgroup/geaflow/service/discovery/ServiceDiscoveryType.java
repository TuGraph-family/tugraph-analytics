package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys;

public enum ServiceDiscoveryType {

    REDIS,

    ZOOKEEPER;

    public static ServiceDiscoveryType getEnum(String type) {
        for (ServiceDiscoveryType serviceType : values()) {
            if (serviceType.name().equalsIgnoreCase(type)) {
                return serviceType;
            }
        }
        return ZOOKEEPER;
    }

    public static ServiceDiscoveryType getEnum(Configuration config) {
        String type = config.getString(ExecutionConfigKeys.SERVICE_DISCOVERY_TYPE);
        return getEnum(type);
    }
}
