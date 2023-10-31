package com.antgroup.geaflow.metaserver.local;

import com.antgroup.geaflow.metaserver.MetaServerContext;
import com.antgroup.geaflow.metaserver.api.NamespaceServiceHandler;
import com.antgroup.geaflow.metaserver.model.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.metaserver.model.protocal.MetaResponse;
import com.antgroup.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.response.DefaultResponse;
import com.antgroup.geaflow.metaserver.model.protocal.response.ServiceResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import com.google.common.collect.Lists;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultServiceHandler implements NamespaceServiceHandler {

    private Map<String, HostAndPort> serviceInfoMap;

    @Override
    public void init(MetaServerContext context) {
        serviceInfoMap = new ConcurrentHashMap<>();
    }

    @Override
    public MetaResponse process(MetaRequest request) {
        switch (request.requestType()) {
            case REGISTER_SERVICE: {
                RegisterServiceRequest registerServiceRequest = (RegisterServiceRequest) request;
                serviceInfoMap.put(registerServiceRequest.getContainerId(),
                    registerServiceRequest.getInfo());
                return new DefaultResponse(true);
            }
            case QUERY_ALL_SERVICE: {
                return new ServiceResponse(Lists.newArrayList(serviceInfoMap.values()));
            }
            default:
                return new DefaultResponse(false, "not support request " + request.requestType());
        }
    }

    @Override
    public NamespaceType namespaceType() {
        return NamespaceType.DEFAULT;
    }
}
