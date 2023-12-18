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

import java.io.Serializable;
import java.util.Objects;

public class ConnectAddress implements Serializable {
    public static final String PORT_SEPARATOR = ":";
    private String host;
    private int port;

    public ConnectAddress() {
    }

    public ConnectAddress(String host, int port) {
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
        ConnectAddress that = (ConnectAddress) o;
        return port == that.port && Objects.equals(host, that.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return host + PORT_SEPARATOR + port;
    }

    public static ConnectAddress build(String address) {
        ConnectAddress rpcAddress = new ConnectAddress();
        String[] hostAndPort = address.split(PORT_SEPARATOR);
        rpcAddress.setHost(hostAndPort[0]);
        rpcAddress.setPort(Integer.parseInt(hostAndPort[1]));
        return rpcAddress;
    }

}
