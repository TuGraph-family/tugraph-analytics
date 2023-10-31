package com.antgroup.geaflow.metaserver.model.protocal.request;

import com.antgroup.geaflow.metaserver.model.protocal.MetaRequestType;
import com.antgroup.geaflow.metaserver.service.NamespaceType;

public class QueryAllServiceRequest extends AbstractMetaRequest {

    public QueryAllServiceRequest(NamespaceType nameSpaceType) {
        super(nameSpaceType);
    }

    public QueryAllServiceRequest() {
        super(NamespaceType.DEFAULT);
    }

    @Override
    public MetaRequestType requestType() {
        return MetaRequestType.QUERY_ALL_SERVICE;
    }
}
