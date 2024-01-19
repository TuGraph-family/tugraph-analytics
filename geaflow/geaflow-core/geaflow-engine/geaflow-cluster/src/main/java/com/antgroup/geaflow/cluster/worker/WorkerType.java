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

package com.antgroup.geaflow.cluster.worker;

public enum WorkerType {
    /**
     * Aligned compute worker type.
     */
    aligned_compute,

    /**
     * Unaligned compute worker type.
     */
    unaligned_compute;

    public static WorkerType getEnum(String value) {
        for (WorkerType v : values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        return aligned_compute;
    }
}
