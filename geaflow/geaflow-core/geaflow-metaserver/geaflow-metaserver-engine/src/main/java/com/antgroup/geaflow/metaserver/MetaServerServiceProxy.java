package com.antgroup.geaflow.metaserver;

import com.antgroup.geaflow.metaserver.service.MetaServerService;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceResultPb;

public class MetaServerServiceProxy implements MetaServerService {

    private final MetaServer metaServer;

    public MetaServerServiceProxy(MetaServer metaServer) {
        this.metaServer = metaServer;
    }

    @Override
    public ServiceResultPb process(ServiceRequestPb request) {
        return metaServer.process(request);
    }
}
