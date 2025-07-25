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

package org.apache.geaflow.state.graph.encoder;

public abstract class BaseBytesEncoder implements IBytesEncoder {

    protected static final int BYTE_SHIFT = 4;
    private static final byte MAGIC_MASK = 0x0F;

    protected byte combine(byte x, byte y) {
        return (byte) ((x << BYTE_SHIFT) | y);
    }

    @Override
    public byte parseMagicNumber(byte b) {
        return (byte) (b & MAGIC_MASK);
    }
}
