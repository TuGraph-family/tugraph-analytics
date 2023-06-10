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

package com.antgroup.geaflow.shuffle.network;

import com.antgroup.geaflow.common.shuffle.ShuffleAddress;
import java.io.Serializable;
import java.net.InetSocketAddress;

public class ConnectionId implements Serializable {

    private static final long serialVersionUID = -8068626194818666857L;

    private final InetSocketAddress address;
    private final int connectionIndex;

    public ConnectionId(ShuffleAddress shuffleAddress, int connectionIndex) {
        this.address = new InetSocketAddress(shuffleAddress.host(), shuffleAddress.port());
        this.connectionIndex = connectionIndex;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public int getConnectionIndex() {
        return connectionIndex;
    }

    @Override
    public int hashCode() {
        return address.hashCode() + (31 * connectionIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other.getClass() != ConnectionId.class) {
            return false;
        }

        final ConnectionId id = (ConnectionId) other;
        return id.getAddress().equals(address) && id.getConnectionIndex() == connectionIndex;
    }

    @Override
    public String toString() {
        return address + " [" + connectionIndex + "]";
    }

}
