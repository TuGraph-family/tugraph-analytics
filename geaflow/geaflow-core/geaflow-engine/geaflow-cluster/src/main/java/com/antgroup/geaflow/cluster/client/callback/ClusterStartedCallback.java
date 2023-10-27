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

package com.antgroup.geaflow.cluster.client.callback;

import com.antgroup.geaflow.cluster.clustermanager.ClusterInfo;
import com.antgroup.geaflow.cluster.rpc.RpcAddress;
import com.antgroup.geaflow.common.utils.ProcessUtil;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public interface ClusterStartedCallback extends Serializable {

    /**
     * The callback for cluster start succeed.
     */
    void onSuccess(ClusterMeta clusterInfo);

    /**
     * The callback for cluster start failed.
     */
    void onFailure(Throwable e);

    class ClusterMeta implements Serializable {

        private String masterAddress;
        private String clientAddress;
        private Map<String, RpcAddress> driverAddresses;

        public ClusterMeta() {
        }

        public ClusterMeta(ClusterInfo clusterInfo) {
            this(clusterInfo.getDriverAddresses(), clusterInfo.getMasterAddress().toString());
        }

        public ClusterMeta(Map<String, RpcAddress> addressList, String masterAddress) {
            this.driverAddresses = new HashMap<>(addressList);
            this.masterAddress = masterAddress;
            this.clientAddress = ProcessUtil.getHostAndIp();
        }

        public String getMasterAddress() {
            return masterAddress;
        }

        public void setMasterAddress(String masterAddress) {
            this.masterAddress = masterAddress;
        }

        public Map<String, RpcAddress> getDriverAddresses() {
            return driverAddresses;
        }

        public void setDriverAddresses(Map<String, RpcAddress> driverAddresses) {
            this.driverAddresses = driverAddresses;
        }

        public String getClientAddress() {
            return clientAddress;
        }

        public void setClientAddress(String clientAddress) {
            this.clientAddress = clientAddress;
        }
    }

}
