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

package com.antgroup.geaflow.operator.impl.graph.algo.vc.msgbox;

import com.antgroup.geaflow.collector.ICollector;
import com.antgroup.geaflow.model.graph.message.DefaultGraphMessage;
import com.antgroup.geaflow.model.graph.message.IGraphMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DirectEmitMsgBox<K, MESSAGE> implements IGraphMsgBox<K, MESSAGE> {

    private final Map<K, List<MESSAGE>> inMessageBox;
    private final ICollector<IGraphMessage<K, MESSAGE>> msgCollector;

    public DirectEmitMsgBox(ICollector<IGraphMessage<K, MESSAGE>> msgCollector) {
        this.inMessageBox = new HashMap<>();
        this.msgCollector = msgCollector;
    }

    @Override
    public void addInMessages(K vertexId, MESSAGE message) {
        List<MESSAGE> messages = inMessageBox.computeIfAbsent(vertexId, k -> new ArrayList<>());
        messages.add(message);
    }

    @Override
    public void processInMessage(MsgProcessFunc<K, MESSAGE> processFunc) {
        processMessage(inMessageBox, processFunc);
    }

    @Override
    public void clearInBox() {
        this.inMessageBox.clear();
    }

    @Override
    public void addOutMessage(K vertexId, MESSAGE message) {
        this.msgCollector.partition(vertexId, new DefaultGraphMessage<>(vertexId, message));
    }

    @Override
    public void processOutMessage(MsgProcessFunc<K, MESSAGE> processFunc) {
    }

    @Override
    public void clearOutBox() {
    }

    private void processMessage(Map<K, List<MESSAGE>> messageBox,
                                MsgProcessFunc<K, MESSAGE> processFunc) {
        for (Entry<K, List<MESSAGE>> entry : messageBox.entrySet()) {
            K vertexId = entry.getKey();
            List<MESSAGE> messageList = entry.getValue();
            processFunc.process(vertexId, messageList);
        }
    }

    private void addMessage(Map<K, List<MESSAGE>> messageBox, K vertexId, MESSAGE message) {
        List<MESSAGE> oldMessages = messageBox.getOrDefault(vertexId, new ArrayList<>());
        oldMessages.add(message);
        messageBox.put(vertexId, oldMessages);
    }

}
