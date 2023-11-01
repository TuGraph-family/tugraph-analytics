package com.antgroup.geaflow.metaserver.model.protocal.request;

import com.antgroup.geaflow.metaserver.model.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.MetaRequestType;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public class RegisterServiceRequest extends AbstractMetaRequest {

    private final String containerId;
    private final HostAndPort info;

    public RegisterServiceRequest(String containerId, HostAndPort info) {
        this(NamespaceType.DEFAULT, containerId, info);
    }

    public RegisterServiceRequest(NamespaceType namespaceType, String containerId,
                                  HostAndPort info) {
        super(namespaceType);
        this.containerId = containerId;
        this.info = info;
    }

    public String getContainerId() {
        return containerId;
    }

    public HostAndPort getInfo() {
        return info;
    }

    @Override
    public MetaRequestType requestType() {
        return MetaRequestType.REGISTER_SERVICE;
    }
}
