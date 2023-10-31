package com.antgroup.geaflow.service.discovery;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceBuilderFactory {

    private static final Map<String, ServiceBuilder> CONCURRENT_TYPE_MAP = new ConcurrentHashMap<>();

    public static synchronized ServiceBuilder build(String serviceType) {
        if (CONCURRENT_TYPE_MAP.containsKey(serviceType)) {
            return CONCURRENT_TYPE_MAP.get(serviceType);
        }

        ServiceLoader<ServiceBuilder> serviceLoader = ServiceLoader.load(ServiceBuilder.class);
        for (ServiceBuilder storeBuilder: serviceLoader) {
            if (storeBuilder.serviceType().equalsIgnoreCase(serviceType)) {
                CONCURRENT_TYPE_MAP.put(serviceType, storeBuilder);
                return storeBuilder;
            }
        }
        throw new GeaflowRuntimeException(RuntimeErrors.INST.spiNotFoundError(serviceType));
    }

    public static synchronized void clear() {
        CONCURRENT_TYPE_MAP.clear();
    }
}
