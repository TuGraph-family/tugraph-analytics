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

package com.antgroup.geaflow.cluster.protocol;

public class OpenContainerResponseEvent implements IEvent {

    private boolean success;
    private int containerId;
    private int firstWorkerIndex;

    public OpenContainerResponseEvent() {
    }

    public OpenContainerResponseEvent(boolean success) {
        this.success = success;
    }

    public OpenContainerResponseEvent(int containerId, int firstWorkerIndex) {
        this.success = true;
        this.containerId = containerId;
        this.firstWorkerIndex = firstWorkerIndex;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getContainerId() {
        return containerId;
    }

    public void setContainerId(int containerId) {
        this.containerId = containerId;
    }

    public int getFirstWorkerIndex() {
        return firstWorkerIndex;
    }

    public void setFirstWorkerIndex(int firstWorkerIndex) {
        this.firstWorkerIndex = firstWorkerIndex;
    }

    @Override
    public EventType getEventType() {
        return EventType.OPEN_CONTAINER_RESPONSE;
    }
}
