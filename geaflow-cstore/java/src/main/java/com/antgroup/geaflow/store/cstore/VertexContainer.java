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

public class VertexContainer {

    public byte[] id;
    public long ts;
    public String label;
    public byte[] property;

    public VertexContainer(byte[] id, long ts, String label, byte[] property) {
        this.id = id;
        this.ts = ts;
        this.label = label;
        this.property = property;
    }

    public VertexContainer(byte[] value) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value);

        int srcIdLen = byteBuffer.getInt();
        byte[] srcId = new byte[srcIdLen];
        byteBuffer.get(srcId);

        long ts = byteBuffer.getLong();

        int labelLen = byteBuffer.getInt();
        byte[] label = new byte[labelLen];
        byteBuffer.get(label);

        int propertyLen = byteBuffer.getInt();
        byte[] property = new byte[propertyLen];
        byteBuffer.get(property);

        this.id = srcId;
        this.ts = ts;
        this.label = new String(label);
        this.property = property;
    }

    @Override
    public String toString() {
        return "VertexContainer{" +
            "id=" + id.length +
            ", ts=" + ts +
            ", label='" + label + '\'' +
            ", property=" + property.length +
            '}';
    }
}
