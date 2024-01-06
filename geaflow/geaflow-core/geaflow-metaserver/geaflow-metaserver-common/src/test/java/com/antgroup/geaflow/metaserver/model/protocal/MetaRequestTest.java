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

package com.antgroup.geaflow.metaserver.model.protocal;

import com.antgroup.geaflow.common.rpc.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.request.QueryAllServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.request.RequestPBConverter;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceRequestPb;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaRequestTest {

    @Test
    public void testQueryAllServiceRequest() {

        QueryAllServiceRequest queryAllServiceRequest = new QueryAllServiceRequest();
        ServiceRequestPb request = RequestPBConverter.convert(queryAllServiceRequest);
        QueryAllServiceRequest serviceRequest = (QueryAllServiceRequest) RequestPBConverter.convert(
            request);

        Assert.assertSame(serviceRequest.requestType(), MetaRequestType.QUERY_ALL_SERVICE);
    }

    @Test
    public void testRegisterServiceRequest() {
        RegisterServiceRequest registerServiceRequest =
            new RegisterServiceRequest(NamespaceType.STATE_SERVICE,"1",
            new HostAndPort("127.0.0.1", 1024));
        ServiceRequestPb request = RequestPBConverter.convert(registerServiceRequest);

        RegisterServiceRequest serviceRequest = (RegisterServiceRequest) RequestPBConverter.convert(
            request);

        Assert.assertEquals(registerServiceRequest.getContainerId(),
            serviceRequest.getContainerId());
        Assert.assertEquals(registerServiceRequest.getInfo(), serviceRequest.getInfo());
        Assert.assertEquals(registerServiceRequest.requestType(), serviceRequest.requestType());
        Assert.assertEquals(registerServiceRequest.namespaceType(), NamespaceType.STATE_SERVICE);
    }

}
