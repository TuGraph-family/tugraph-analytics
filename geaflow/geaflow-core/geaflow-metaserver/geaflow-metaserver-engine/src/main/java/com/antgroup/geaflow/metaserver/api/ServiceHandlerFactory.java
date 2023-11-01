package com.antgroup.geaflow.metaserver.api;

import com.antgroup.geaflow.metaserver.MetaServerContext;
import com.antgroup.geaflow.metaserver.local.DefaultServiceHandler;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceHandlerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceHandlerFactory.class);

    public static synchronized Map<NamespaceType, NamespaceServiceHandler> load(
        MetaServerContext context) {

        Map<NamespaceType, NamespaceServiceHandler> map = Maps.newConcurrentMap();
        ServiceLoader<NamespaceServiceHandler> serviceLoader = ServiceLoader.load(
            NamespaceServiceHandler.class);
        for (NamespaceServiceHandler handler : serviceLoader) {
            LOGGER.info("{} register service handler", handler.namespaceType());
            handler.init(context);
            map.put(handler.namespaceType(), handler);
        }
        DefaultServiceHandler defaultServiceHandler = new DefaultServiceHandler();
        defaultServiceHandler.init(context);
        map.put(defaultServiceHandler.namespaceType(), defaultServiceHandler);
        return map;
    }
}
