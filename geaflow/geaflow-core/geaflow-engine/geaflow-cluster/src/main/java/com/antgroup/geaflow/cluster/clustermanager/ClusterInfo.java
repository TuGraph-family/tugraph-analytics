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
import java.util.Objects;

public class ClusterInfo implements Serializable {

    private RpcAddress masterAddress;

    private RpcAddress driverAddress;

    public ClusterInfo() {
    }

    public ClusterInfo(RpcAddress masterAddress, RpcAddress driverAddress) {
        this.masterAddress = masterAddress;
        this.driverAddress = driverAddress;
    }

    public RpcAddress getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(RpcAddress masterAddress) {
        this.masterAddress = masterAddress;
    }

    public RpcAddress getDriverAddress() {
        return driverAddress;
    }

    public void setDriverAddress(RpcAddress driverAddress) {
        this.driverAddress = driverAddress;
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
            .equals(driverAddress, that.driverAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterAddress, driverAddress);
    }

    @Override
    public String toString() {
        return "ClusterInfo{" + "masterAddress=" + masterAddress.getAddress() + ", driverAddress="
            + driverAddress.getAddress() + '}';
    }

}
