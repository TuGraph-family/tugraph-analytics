package com.antgroup.geaflow.cluster.master;

import com.antgroup.geaflow.metaserver.MetaServer;
import com.antgroup.geaflow.metaserver.MetaServerContext;

public class ServiceMaster extends AbstractMaster {

    private MetaServer metaServer;

    @Override
    public void init(MasterContext context) {
        super.init(context);
    }

    @Override
    protected void initEnv(MasterContext context) {
        // Start meta server.
        this.metaServer = new MetaServer();
        MetaServerContext metaServerContext = new MetaServerContext(context.getConfiguration());
        metaServerContext.setRecover(clusterContext.isRecover());
        this.metaServer.init(metaServerContext);

        super.initEnv(context);
    }

    @Override
    public void close() {
        super.close();
        metaServer.close();
    }
}
