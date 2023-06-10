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

public enum MessageType {
    /**
     * The message type for {@link IPathMessage} which is the path message send to the vertex.
     */
    PATH,
    /**
     * The message type for {@link EODMessage} which is end-of-data message sending to the next
     * by the step operator when finished processing all the data.
     */
    EOD,
    /**
     * The message type for {@link ParameterRequestMessage} which is the request message with the parameters.
     */
    PARAMETER_REQUEST,
    /**
     * The path message type for {@link JoinPathMessage} which  for the
     * {@link com.antgroup.geaflow.dsl.runtime.traversal.operator.StepJoinOperator}.
     */
    JOIN_PATH,
    /**
     * The message type for {@link KeyGroupMessage} which is the message send to the
     * {@link com.antgroup.geaflow.dsl.common.data.VirtualId} to do same relation operations
     * e.g. aggregate.
     */
    KEY_GROUP,
    /**
     * The message type for {@link ReturnMessage} which is the result returning from the sub-query calling.
     */
    RETURN_VALUE;

    public SingleMessageBox createMessageBox() {
        switch (this) {
            case PATH:
                return new PathMessageBox();
            case EOD:
                return new EODMessageBox();
            case PARAMETER_REQUEST:
                return new ParameterRequestMessageBox();
            case JOIN_PATH:
                return new JoinPathMessageBox();
            case KEY_GROUP:
                return new KeyGroupMessageBox();
            case RETURN_VALUE:
                return new ReturnMessageBox();
            default:
                throw new IllegalArgumentException("Failing to create message box for: " + this);
        }
    }
}
