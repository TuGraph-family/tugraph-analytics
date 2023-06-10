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

import it.unimi.dsi.fastutil.longs.LongArraySet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UnionMessageBox implements MessageBox {

    private final Map<MessageType, SingleMessageBox> messageBoxes;

    public UnionMessageBox(Map<MessageType, SingleMessageBox> messageBoxes) {
        this.messageBoxes = Objects.requireNonNull(messageBoxes);
    }

    public UnionMessageBox() {
        this(new HashMap<>());
    }

    @Override
    public void addMessage(long receiverId, IMessage message) {
        MessageType messageType = message.getType();
        MessageBox messageBox = messageBoxes.computeIfAbsent(messageType, m -> messageType.createMessageBox());
        messageBox.addMessage(receiverId, message);
    }

    @Override
    public <M extends IMessage> M getMessage(long receiverId, MessageType messageType) {
        MessageBox messageBox = messageBoxes.get(messageType);
        if (messageBox != null) {
            return messageBox.getMessage(receiverId, messageType);
        }
        return null;
    }

    @Override
    public long[] getReceiverIds() {
        LongArraySet receiverIds = new LongArraySet();
        for (SingleMessageBox messageBox : messageBoxes.values()) {
            for (long id : messageBox.getReceiverIds()) {
                receiverIds.add(id);
            }
        }
        return receiverIds.toLongArray();
    }

    public void addMessageBox(MessageBox other) {
        if (other instanceof UnionMessageBox) {
            UnionMessageBox otherUnionBox = (UnionMessageBox) other;
            for (MessageType messageType : otherUnionBox.messageBoxes.keySet()) {
                SingleMessageBox otherBox = otherUnionBox.getMessageBox(messageType);
                addMessageBox(otherBox);
            }
        } else { // add single message box.
            SingleMessageBox singleBox = (SingleMessageBox) other;
            MessageType messageType = singleBox.getMessageType();
            if (messageBoxes.containsKey(messageType)) {
                SingleMessageBox combineBox = (SingleMessageBox) messageBoxes.get(messageType).combine(other);
                messageBoxes.put(messageType, combineBox);
            } else {
                messageBoxes.put(messageType, singleBox);
            }
        }
    }

    public SingleMessageBox getMessageBox(MessageType messageType) {
        return messageBoxes.get(messageType);
    }

    @Override
    public MessageBox combine(MessageBox other) {
        UnionMessageBox newBox = this.copy();
        newBox.addMessageBox(other);
        return newBox;
    }

    @Override
    public UnionMessageBox copy() {
        return new UnionMessageBox(new HashMap<>(messageBoxes));
    }
}
