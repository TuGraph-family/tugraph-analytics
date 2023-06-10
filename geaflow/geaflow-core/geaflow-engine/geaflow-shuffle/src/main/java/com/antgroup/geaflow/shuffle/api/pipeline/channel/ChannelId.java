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

package com.antgroup.geaflow.shuffle.api.pipeline.channel;

import io.netty.buffer.ByteBuf;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * This class is an adaptation of Flink's org.apache.flink.util.AbstractID.
 */
public class ChannelId implements Serializable {

    private static final long serialVersionUID = 2L;
    // The upper part of the actual ID.
    private final long upperPart;
    // The lower part of the actual ID.
    private final long lowerPart;

    public ChannelId() {
        UUID uuid = UUID.randomUUID();
        this.upperPart = uuid.getMostSignificantBits();
        this.lowerPart = uuid.getLeastSignificantBits();
    }

    public ChannelId(long lowerPart, long upperPart) {
        this.upperPart = upperPart;
        this.lowerPart = lowerPart;
    }

    public void writeTo(ByteBuf buf) {
        buf.writeLong(this.lowerPart);
        buf.writeLong(this.upperPart);
    }

    public static ChannelId readFrom(ByteBuf buf) {
        long lower = buf.readLong();
        long upper = buf.readLong();
        return new ChannelId(lower, upper);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChannelId that = (ChannelId) o;
        return upperPart == that.upperPart && lowerPart == that.lowerPart;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upperPart, lowerPart);
    }
}
