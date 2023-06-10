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

package com.antgroup.geaflow.dsl.runtime.traversal.message;

import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class EODMessage implements IMessage {

    private final List<EndOfData> endOfDatas;

    private EODMessage() {
        this(new ArrayList<>());
    }

    private EODMessage(List<EndOfData> endOfDatas) {
        this.endOfDatas = Objects.requireNonNull(endOfDatas);
    }

    public static EODMessage of(EndOfData data) {
        return new EODMessage(Lists.newArrayList(data));
    }

    @Override
    public MessageType getType() {
        return MessageType.EOD;
    }

    @Override
    public IMessage combine(IMessage other) {
        EODMessage newMessage = this.copy();
        EODMessage otherEod = (EODMessage) other;
        newMessage.endOfDatas.addAll(otherEod.endOfDatas);
        return newMessage;
    }

    @Override
    public EODMessage copy() {
        return new EODMessage(new ArrayList<>(endOfDatas));
    }

    public List<EndOfData> getEodData() {
        return endOfDatas;
    }
}
