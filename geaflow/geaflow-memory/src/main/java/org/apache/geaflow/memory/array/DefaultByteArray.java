/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.memory.array;

import java.nio.ByteBuffer;

public class DefaultByteArray implements ByteArray {

    private byte[] data;
    private final int offset;
    private final int length;

    public DefaultByteArray(byte[] data) {
        this(data, 0, data.length);
    }

    public DefaultByteArray(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public byte[] array() {
        return data;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(data, offset, length);
    }

    @Override
    public boolean release() {
        data = null;
        return true;
    }
}
