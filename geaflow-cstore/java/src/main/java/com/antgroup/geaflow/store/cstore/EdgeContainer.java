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
package com.antgroup.geaflow.store.cstore;

import java.nio.ByteBuffer;

public class EdgeContainer {

    public byte[] sid;
    public long ts;
    public String label;
    public boolean isOut;
    public byte[] tid;
    public byte[] property;

    public EdgeContainer(byte[] sid, long ts, String label, boolean isOut, byte[] tid,
                         byte[] property) {
        this.sid = sid;
        this.ts = ts;
        this.label = label;
        this.isOut = isOut;
        this.tid = tid;
        this.property = property;
    }

    public EdgeContainer(byte[] value) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value);

        int srcIdLen = byteBuffer.getInt();
        byte[] srcId = new byte[srcIdLen];
        byteBuffer.get(srcId);

        int targetIdLen = byteBuffer.getInt();
        byte[] targetId = new byte[targetIdLen];
        byteBuffer.get(targetId);

        long ts = byteBuffer.getLong();

        int labelLen = byteBuffer.getInt();
        byte[] label = new byte[labelLen];
        byteBuffer.get(label);

        int directionInt = byteBuffer.get();

        int propertyLen = byteBuffer.getInt();
        byte[] property = new byte[propertyLen];
        byteBuffer.get(property);

        this.sid = srcId;
        this.tid = targetId;
        this.ts = ts;
        this.label = new String(label);
        this.isOut = directionInt != 0;
        this.property = property;
    }
}
