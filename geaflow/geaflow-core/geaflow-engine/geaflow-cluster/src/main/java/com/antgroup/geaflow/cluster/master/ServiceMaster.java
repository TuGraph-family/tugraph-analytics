/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

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
