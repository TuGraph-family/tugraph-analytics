package com.antgroup.geaflow.metaserver.service;


import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceResultPb;

public interface MetaServerService {

    /**
     * Process all messages sent to meta server and return results.
     */
    ServiceResultPb process(ServiceRequestPb request);

}
