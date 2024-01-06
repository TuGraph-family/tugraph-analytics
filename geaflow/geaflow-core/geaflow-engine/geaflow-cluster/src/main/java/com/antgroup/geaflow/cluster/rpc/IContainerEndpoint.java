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

package com.antgroup.geaflow.cluster.rpc;

import com.antgroup.geaflow.rpc.proto.Container.Request;
import com.antgroup.geaflow.rpc.proto.Container.Response;
import com.google.protobuf.Empty;
import java.io.Serializable;

public interface IContainerEndpoint extends Serializable {

    /**
     * Container process.
     */
    Response process(Request request);

    /**
     * Container close.
     */
    Empty close(Empty request);

}
