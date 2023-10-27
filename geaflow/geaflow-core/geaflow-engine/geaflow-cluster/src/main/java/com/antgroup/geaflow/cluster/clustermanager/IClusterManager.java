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

package com.antgroup.geaflow.cluster.clustermanager;

import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import java.io.Serializable;
import java.util.Map;

public interface IClusterManager extends Serializable {

    /**
     * Initialize cluster manager.
     */
    void init(ClusterContext context);

    /**
     * Start master.
     */
    ClusterId startMaster();

    /**
     * Start driver.
     */
    Map<String, RpcAddress> startDrivers();

    /**
     * Start workers.
     */
    void allocateWorkers(int workerNum);

    /**
     * Restart container of container id.
     */
    void restartContainer(int containerId);

    /**
     * Trigger job failover.
     */
    void doFailover(int componentId, Throwable cause);

    /**
     * Close cluster manager.
     */
    void close();

}
