/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

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
