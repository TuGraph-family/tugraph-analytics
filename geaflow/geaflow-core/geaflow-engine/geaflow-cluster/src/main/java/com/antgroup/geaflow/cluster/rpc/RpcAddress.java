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

package com.antgroup.geaflow.cluster.rpc;

import static com.antgroup.geaflow.cluster.constants.ClusterConstants.PORT_SEPARATOR;

import java.io.Serializable;
import java.util.Objects;

public class RpcAddress implements Serializable {

    private String host;
    private int port;

    public RpcAddress() {
    }

    public RpcAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RpcAddress that = (RpcAddress) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    public String getAddress() {
        return host + PORT_SEPARATOR + port;
    }

    @Override
    public String toString() {
        return "RpcAddress{" + "host='" + host + '\'' + ", port=" + port + '}';
    }

    public static RpcAddress build(String address) {
        RpcAddress rpcAddress = new RpcAddress();
        String[] hostAndPort = address.split(PORT_SEPARATOR);
        rpcAddress.setHost(hostAndPort[0]);
        rpcAddress.setPort(Integer.parseInt(hostAndPort[1]));
        return rpcAddress;
    }

}
