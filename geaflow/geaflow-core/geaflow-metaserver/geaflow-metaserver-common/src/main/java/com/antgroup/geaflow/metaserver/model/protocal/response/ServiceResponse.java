package com.antgroup.geaflow.metaserver.model.protocal.response;

import com.antgroup.geaflow.metaserver.model.HostAndPort;
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
