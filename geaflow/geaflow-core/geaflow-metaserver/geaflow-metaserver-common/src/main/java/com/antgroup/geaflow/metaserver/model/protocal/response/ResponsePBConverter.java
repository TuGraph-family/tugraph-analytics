package com.antgroup.geaflow.metaserver.model.protocal.response;

import com.antgroup.geaflow.common.encoder.RpcMessageEncoder;
import com.antgroup.geaflow.metaserver.model.protocal.MetaResponse;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import com.google.protobuf.ByteString;

public class ResponsePBConverter {

    public static ServiceResultPb convert(MetaResponse response) {
        ByteString bytes = RpcMessageEncoder.encode(response);
        return ServiceResultPb.newBuilder().setResult(bytes).build();
    }

    public static MetaResponse convert(ServiceResultPb result) {
        return RpcMessageEncoder.decode(result.getResult());
    }
}
