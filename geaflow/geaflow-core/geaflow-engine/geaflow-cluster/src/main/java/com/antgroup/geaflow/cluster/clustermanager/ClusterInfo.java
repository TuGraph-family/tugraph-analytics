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
import java.util.Objects;

public class ClusterInfo implements Serializable {

    private RpcAddress masterAddress;
    private Map<String, RpcAddress> driverAddresses;

    public ClusterInfo() {
    }

    public RpcAddress getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(RpcAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public Map<String, RpcAddress> getDriverAddresses() {
        return driverAddresses;
    }

    public void setDriverAddresses(Map<String, RpcAddress> driverAddresses) {
        this.driverAddresses = driverAddresses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterInfo that = (ClusterInfo) o;
        return Objects.equals(masterAddress, that.masterAddress) && Objects
            .equals(driverAddresses, that.driverAddresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterAddress, driverAddresses);
    }

    @Override
    public String toString() {
        return "ClusterInfo{" + "masterAddress=" + masterAddress + ", driverAddresses="
            + driverAddresses + '}';
    }

}
