package com.antgroup.geaflow.metaserver.client;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.metaserver.model.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.request.QueryAllServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.response.ServiceResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;
import java.util.List;

public class MetaServerQueryClient extends BaseClient {

    private static MetaServerQueryClient client;

    public MetaServerQueryClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerQueryClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerQueryClient(configuration);
        }
        return client;
    }


    public List<HostAndPort> queryAllServices(NamespaceType namespaceType) {
        ServiceResponse response = process(new QueryAllServiceRequest(namespaceType));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
        return response.getServiceInfos();
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
