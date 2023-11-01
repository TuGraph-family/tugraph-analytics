package com.antgroup.geaflow.metaserver.model.protocal;

import com.antgroup.geaflow.metaserver.model.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.response.DefaultResponse;
import com.antgroup.geaflow.metaserver.model.protocal.response.ResponsePBConverter;
import com.antgroup.geaflow.metaserver.model.protocal.response.ServiceResponse;
import com.antgroup.geaflow.rpc.proto.MetaServer.ServiceResultPb;
import com.google.common.collect.Lists;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaResponseTest {

    @Test
    public void testDefaultResponse() {
        DefaultResponse defaultResponse = new DefaultResponse(true);
        ServiceResultPb result = ResponsePBConverter.convert(defaultResponse);
        DefaultResponse response = (DefaultResponse) ResponsePBConverter.convert(result);

        Assert.assertTrue(response.isSuccess());

        defaultResponse = new DefaultResponse(false, "error");
        result = ResponsePBConverter.convert(defaultResponse);
        response = (DefaultResponse) ResponsePBConverter.convert(result);

        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getMessage(), "error");
    }

    @Test
    public void testServiceResponse() {
        ServiceResponse serviceResponse = new ServiceResponse(false, "error");
        ServiceResultPb result = ResponsePBConverter.convert(serviceResponse);
        ServiceResponse response = (ServiceResponse) ResponsePBConverter.convert(result);

        Assert.assertFalse(response.isSuccess());
        Assert.assertEquals(response.getMessage(), "error");

        List<HostAndPort> HostAndPortList = Lists.newArrayList(new HostAndPort("127.0.0.1", 1024),
            new HostAndPort("127.0.0.1", 1025));

        serviceResponse = new ServiceResponse(HostAndPortList);
        result = ResponsePBConverter.convert(serviceResponse);
        response = (ServiceResponse) ResponsePBConverter.convert(result);

        Assert.assertEquals(serviceResponse.getServiceInfos(), response.getServiceInfos());
        Assert.assertEquals(serviceResponse.isSuccess(), response.isSuccess());

    }

}
