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

package com.antgroup.geaflow.cluster.resourcemanager.allocator;

import com.antgroup.geaflow.cluster.resourcemanager.WorkerInfo;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class RoundRobinAllocator extends AbstractAllocator<String, WorkerInfo> {

    @Override
    public List<WorkerInfo> doAllocate(int num) {

        List<WorkerInfo> allocated = new ArrayList<>();
        int n = 0;
        Iterator<Comparable<String>> groupIterator = this.group2workers.keySet().iterator();
        while (n < num) {
            if (groupIterator.hasNext()) {
                Comparable<String> group = groupIterator.next();
                LinkedList<WorkerInfo> next = this.group2workers.get(group);
                if (!next.isEmpty()) {
                    allocated.add(next.pollFirst());
                    n++;
                }
            } else {
                groupIterator = this.group2workers.keySet().iterator();
            }
        }

        return allocated;
    }

    @Override
    public AllocateStrategy getStrategy() {
        return AllocateStrategy.ROUND_ROBIN;
    }

    @Override
    public WorkerGroupByFunction<String, WorkerInfo> getWorkerGroupByFunction() {
        return PROC_GROUP_SELECTOR;
    }

}

