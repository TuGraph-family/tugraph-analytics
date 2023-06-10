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

package com.antgroup.geaflow.partitioner;

import com.antgroup.geaflow.api.partition.IPartition;
import java.io.Serializable;

public interface IPartitioner<T> extends Serializable {

    /**
     * Returns op id.
     */
    int getOpId();

    /**
     * Returns the partition.
     */
    IPartition<T> getPartition();

    /**
     * Returns the partition type.
     */
    PartitionType getPartitionType();

    enum PartitionType {

        /**
         * Random partition.
         */
        forward(true),
        /**
         * Broadcast partition.
         */
        broadcast(true),
        /**
         * Key partition.
         */
        key(true),
        /**
         * Custom partition.
         */
        custom(false),
        /**
         * Iterator partition.
         */
        iterator(false);

        boolean enablePushUp;
        PartitionType(boolean enablePushUp) {
            this.enablePushUp = enablePushUp;
        }

        public boolean isEnablePushUp() {
            return enablePushUp;
        }
    }

}
