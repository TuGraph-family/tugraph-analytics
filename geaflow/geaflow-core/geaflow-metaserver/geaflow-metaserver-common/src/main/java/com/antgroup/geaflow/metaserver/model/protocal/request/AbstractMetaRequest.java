package com.antgroup.geaflow.metaserver.model.protocal.request;

import com.antgroup.geaflow.metaserver.model.protocal.MetaRequest;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public abstract class AbstractMetaRequest implements MetaRequest {

    private final NamespaceType nameSpaceType;

    public AbstractMetaRequest(NamespaceType nameSpaceType) {
        this.nameSpaceType = nameSpaceType;
    }

    @Override
    public NamespaceType namespaceType() {
        return nameSpaceType;
    }
}
