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

package com.antgroup.geaflow.api.partition.kv;

import com.antgroup.geaflow.api.partition.IPartition;
import java.util.stream.IntStream;

public class BroadCastPartition<T> implements IPartition<T> {

    private int[] partitions = null;

    @Override
    public int[] partition(T value, int numPartition) {
        if (partitions != null) {
            return partitions;
        } else {
            partitions = IntStream.rangeClosed(0, numPartition - 1).toArray();
            return partitions;
        }
    }
}
