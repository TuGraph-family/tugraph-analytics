package com.antgroup.geaflow.metaserver.model.protocal.request;

import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import com.google.protobuf.ByteString;

public class RequestPBConverter {

    public static ServiceRequestPb convert(MetaRequest request) {
        ByteString bytes = RpcMessageEncoder.encode(request);
        return ServiceRequestPb.newBuilder().setRequest(bytes).build();
    }

    public static MetaRequest convert(ServiceRequestPb request) {
        return RpcMessageEncoder.decode(request.getRequest());
    }


}
