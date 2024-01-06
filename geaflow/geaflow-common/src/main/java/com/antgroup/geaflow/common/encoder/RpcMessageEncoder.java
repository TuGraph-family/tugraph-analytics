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

package com.antgroup.geaflow.common.encoder;

import com.antgroup.geaflow.common.serialize.SerializerFactory;
import com.google.protobuf.ByteString;
import java.io.Serializable;

public class RpcMessageEncoder implements Serializable {

    public static <T> T decode(ByteString payload) {
        return SerializerFactory.getKryoSerializer().deserialize(payload.newInput());
    }

    public static <T> ByteString encode(T request) {
        ByteString.Output output = ByteString.newOutput();
        SerializerFactory.getKryoSerializer().serialize(request, output);
        return output.toByteString();
    }
}
