package com.antgroup.geaflow.metaserver.client.interal;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.metaserver.client.BaseClient;
import com.antgroup.geaflow.metaserver.model.HostAndPort;
import com.antgroup.geaflow.metaserver.model.protocal.request.RegisterServiceRequest;
import com.antgroup.geaflow.metaserver.model.protocal.response.DefaultResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public class MetaServerClient extends BaseClient {

    private static MetaServerClient client;

    public MetaServerClient() {

    }

    public MetaServerClient(Configuration configuration) {
        super(configuration);
    }

    public static synchronized MetaServerClient getClient(Configuration configuration) {
        if (client == null) {
            client = new MetaServerClient(configuration);
        }
        return client;
    }

    public void registerService(NamespaceType namespaceType, String containerId,
                                HostAndPort serviceInfo) {
        DefaultResponse response = process(
            new RegisterServiceRequest(namespaceType, containerId, serviceInfo));
        if (!response.isSuccess()) {
            throw new GeaflowRuntimeException(response.getMessage());
        }
    }

    @Override
    public void close() {
        super.close();
        client = null;
    }
}
