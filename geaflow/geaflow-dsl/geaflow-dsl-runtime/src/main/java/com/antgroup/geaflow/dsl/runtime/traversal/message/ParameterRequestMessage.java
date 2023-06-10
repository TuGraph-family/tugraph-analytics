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

import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class ParameterRequestMessage implements IMessage {

    private final Set<ParameterRequest> requests;

    public ParameterRequestMessage(Set<ParameterRequest> requests) {
        this.requests = requests;
    }

    public ParameterRequestMessage() {
        this(new HashSet<>());
    }

    @Override
    public MessageType getType() {
        return MessageType.PARAMETER_REQUEST;
    }

    public void addRequest(ParameterRequest request) {
        requests.add(request);
    }

    public void forEach(Consumer<ParameterRequest> consumer) {
        requests.forEach(consumer);
    }

    @Override
    public IMessage combine(IMessage other) {

        Set<ParameterRequest> requests = new HashSet<>();
        if (this.requests != null) {
            requests.addAll(this.requests);
        }
        Set<ParameterRequest> thatRequests = ((ParameterRequestMessage) other).requests;
        if (thatRequests != null) {
            requests.addAll(thatRequests);
        }
        return new ParameterRequestMessage(requests);
    }

    @Override
    public IMessage copy() {
        return new ParameterRequestMessage(new HashSet<>(requests));
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }
}
