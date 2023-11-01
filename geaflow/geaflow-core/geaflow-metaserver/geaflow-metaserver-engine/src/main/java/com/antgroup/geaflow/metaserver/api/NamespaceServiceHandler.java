package com.antgroup.geaflow.metaserver.api;

import com.antgroup.geaflow.metaserver.MetaServerContext;
import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.metaserver.model.protocal.MetaResponse;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public interface NamespaceServiceHandler {

    /**
     * Init service handler.
     */
    void init(MetaServerContext context);

    /**
     * Process meta request and return response.
     */
    MetaResponse process(MetaRequest request);

    /**
     * Namespace of handler.
     */
    NamespaceType namespaceType();
}
