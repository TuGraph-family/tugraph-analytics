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

package com.antgroup.geaflow.state.graph.encoder;

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
