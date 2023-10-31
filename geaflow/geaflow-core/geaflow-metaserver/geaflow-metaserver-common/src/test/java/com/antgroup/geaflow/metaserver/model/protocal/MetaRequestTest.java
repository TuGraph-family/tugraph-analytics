package com.antgroup.geaflow.metaserver.model.protocal;

import com.antgroup.geaflow.metaserver.model.HostAndPort;
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
