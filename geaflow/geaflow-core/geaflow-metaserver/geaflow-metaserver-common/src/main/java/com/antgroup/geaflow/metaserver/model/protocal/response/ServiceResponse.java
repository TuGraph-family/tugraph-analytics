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

import com.antgroup.geaflow.common.rpc.HostAndPort;
import java.util.List;

public class ServiceResponse extends DefaultResponse {

    private List<HostAndPort> serviceInfos;

    public ServiceResponse(boolean success, String message) {
        super(success, message);
    }

    public ServiceResponse(List<HostAndPort> serviceInfos) {
        super(true);
        this.serviceInfos = serviceInfos;
    }

    public List<HostAndPort> getServiceInfos() {
        return serviceInfos;
    }
}
