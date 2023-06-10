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

package com.antgroup.geaflow.state.graph;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;

public enum StateMode {
    /**
     * normal state.
     */
    RW((short)0),
    /**
     * read only state for state sharing.
     */
    RDONLY((short)1),
    /**
     * copy on write state for cold start.
     */
    COW((short)2);

    private final short flag;

    StateMode(short flag) {
        this.flag = flag;
    }

    public short value() {
        return flag;
    }

    public static StateMode getEnum(String value) {
        for (StateMode v : values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        throw new GeaflowRuntimeException("not support " + value);
    }
}
