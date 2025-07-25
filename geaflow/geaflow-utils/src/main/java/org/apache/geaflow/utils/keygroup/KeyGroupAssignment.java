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

package org.apache.geaflow.utils.keygroup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.geaflow.utils.math.MathUtil;

/**
 * This class is an adaptation of Flink's org.apache.flink.runtime.state.KeyGroupRangeAssignment.
 */
public final class KeyGroupAssignment {

    /**
     * Computes the range of key-groups that are assigned to a given operator under the given
     * parallelism and maximum parallelism.
     * @param maxParallelism Maximal parallelism that the job was initially created with.
     * @param parallelism The current parallelism under which the job runs. Must be <=
     *     maxParallelism.
     * @param index Id of a key-group. 0 <= keyGroupID < maxParallelism.
     */
    public static KeyGroup computeKeyGroupRangeForOperatorIndex(int maxParallelism, int parallelism,
                                                                                                int index) {
        Preconditions.checkArgument(maxParallelism > 0, "maxParallelism should be > 0");
        if (parallelism > maxParallelism)  {
            throw new IllegalArgumentException("Maximum parallelism " + maxParallelism + " must "
                + "not be smaller than parallelism " + parallelism);
        }
        int start = index == 0 ? 0 : ((index * maxParallelism - 1) / parallelism) + 1;
        int end = ((index + 1) * maxParallelism - 1) / parallelism;
        return new KeyGroup(start, end);
    }

    /**
     * Assigns the given key to a parallel operator index.
     * @param key the key to assign
     * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
     * @param parallelism the current parallelism of the operator
     * @return the index of the parallel operator to which the given key should be routed.
     */
    public static int assignKeyToParallelTask(Object key, int maxParallelism, int parallelism) {
        return computeTaskIndexForKeyGroup(maxParallelism, parallelism,
            assignToKeyGroup(key, maxParallelism));
    }

    /**
     * Computes the index of the operator to which a key-group belongs under the given parallelism
     * and maximum parallelism.
     * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this
     * method. If we ever want
     * to go beyond this boundary, this method must perform arithmetic on long values.
     * @param maxParallelism Maximal parallelism that the job was initially created with.
     *     0 < parallelism <= maxParallelism <= Short.MAX_VALUE must hold.
     * @param parallelism The current parallelism under which the job runs. Must be <=
     *     maxParallelism.
     * @param keyGroupId Id of a key-group. 0 <= keyGroupID < maxParallelism.
     * @return The index of the operator to which elements from the given key-group should be routed
     *     under the given parallelism and maxParallelism.
     */
    public static int computeTaskIndexForKeyGroup(int maxParallelism, int parallelism,
                                                  int keyGroupId) {
        Preconditions.checkArgument(maxParallelism > 0, "maxParallelism should be > 0");
        if (parallelism > maxParallelism)  {
            throw new IllegalArgumentException("Maximum parallelism " + maxParallelism + " must "
                + "not be smaller than parallelism " + parallelism);
        }
        return keyGroupId * parallelism / maxParallelism;
    }

    /**
     * Assigns the given key to a key-group index.
     * @param key the key to assign
     * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
     * @return the key-group to which the given key is assigned
     */
    public static int assignToKeyGroup(Object key, int maxParallelism) {
        return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
    }

    /**
     * Assigns the given key to a key-group index.
     * @param keyHash the hash of the key to assign
     * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
     * @return the key-group to which the given key is assigned
     */
    public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
        // we can rehash keyHash
        return MathUtil.murmurHash(keyHash) % maxParallelism;
    }

    @VisibleForTesting
    public static Map<Integer, List<Integer>> computeKeyGroupToTask(int maxParallelism,
                                                                    List<Integer> targetTasks) {
        Map<Integer, List<Integer>> keyGroupToTask = new ConcurrentHashMap<>();
        for (int index = 0; index < targetTasks.size(); index++) {
            KeyGroup taskKeyGroup = computeKeyGroupRangeForOperatorIndex(maxParallelism,
                targetTasks.size(), index);
            for (int groupId = taskKeyGroup.getStartKeyGroup();
                groupId <= taskKeyGroup.getEndKeyGroup(); groupId++) {
                keyGroupToTask.put(groupId, ImmutableList.of(targetTasks.get(index)));
            }
        }
        return keyGroupToTask;
    }

}
