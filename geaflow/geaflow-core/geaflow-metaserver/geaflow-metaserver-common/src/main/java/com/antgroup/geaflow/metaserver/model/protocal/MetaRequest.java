package com.antgroup.geaflow.metaserver.model.protocal;

import com.antgroup.geaflow.metaserver.service.NamespaceType;

public interface MetaRequest {

    NamespaceType namespaceType();

    MetaRequestType requestType();
}
