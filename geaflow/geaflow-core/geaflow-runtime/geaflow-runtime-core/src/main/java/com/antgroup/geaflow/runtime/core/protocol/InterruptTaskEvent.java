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

package com.antgroup.geaflow.runtime.core.protocol;

import com.antgroup.geaflow.cluster.protocol.EventType;
import com.antgroup.geaflow.cluster.protocol.ICommand;

public class InterruptTaskEvent implements ICommand {

    // Worker id to execute event.
    protected final int workerId;

    // Cycle id of the current event.
    protected final int cycleId;


    public InterruptTaskEvent(int workerId, int cycleId) {
        this.workerId = workerId;
        this.cycleId = cycleId;
    }

    @Override
    public int getWorkerId() {
        return workerId;
    }

    @Override
    public EventType getEventType() {
        return EventType.INTERRUPT_TASK;
    }

    public int getCycleId() {
        return cycleId;
    }

    @Override
    public String toString() {
        return "InterruptTaskEvent{"
            + "workerId=" + workerId
            + ", cycleId=" + cycleId
            + '}';
    }
}
