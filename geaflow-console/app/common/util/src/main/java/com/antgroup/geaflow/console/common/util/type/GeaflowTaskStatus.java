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

package com.antgroup.geaflow.console.common.util.type;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;

public enum GeaflowTaskStatus {

    CREATED,

    /**
     * submitting, but not scheduled to the engine.
     */
    WAITING,

    /**
     * submitting, scheduled to the engine.
     */
    STARTING,

    FAILED,

    RUNNING,

    FINISHED,

    STOPPED,

    DELETED;

    public void checkOperation(GeaflowOperationType operationType) {
        switch (operationType) {
            case START:
                switch (this) {
                    case CREATED:
                    case FAILED:
                    case STOPPED:
                        break;
                    default:
                        throw new GeaflowException("Task {} status can't {}", this, operationType);
                }
                break;
            case STOP:
                switch (this) {
                    case RUNNING:
                    case WAITING:
                        break;
                    default:
                        throw new GeaflowException("Task {} status can't {}", this, operationType);
                }
                break;
            case REFRESH:
                break;
            case PUBLISH:
            case RESET:
            case DELETE:
                switch (this) {
                    case CREATED:
                    case FAILED:
                    case STOPPED:
                    case FINISHED:
                        break;
                    default:
                        throw new GeaflowException("Task {} status can't {}", this, operationType);
                }
                break;
            default:
                throw new GeaflowException("Unsupported operation {} on task", operationType);
        }
    }
}
