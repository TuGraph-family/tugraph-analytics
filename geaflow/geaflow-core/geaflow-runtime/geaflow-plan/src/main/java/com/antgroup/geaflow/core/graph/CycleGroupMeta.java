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

package com.antgroup.geaflow.core.graph;

import com.antgroup.geaflow.plan.graph.AffinityLevel;

/**
 * The CycleGroupMeta which is used to describe group meta(e.g loop count, whether is iterative) of cycle.
 */
public class CycleGroupMeta {

    private long iterationCount;
    private int flyingCount;
    private boolean isIterative;
    private AffinityLevel affinityLevel;

    public CycleGroupMeta() {
        this.iterationCount = 1;
        this.flyingCount = 1;
        this.isIterative = false;
        this.affinityLevel = AffinityLevel.worker;
    }

    public long getIterationCount() {
        return iterationCount;
    }

    public void setIterationCount(long iterationCount) {
        this.iterationCount = iterationCount;
    }

    public int getFlyingCount() {
        return flyingCount;
    }

    public void setFlyingCount(int flyingCount) {
        this.flyingCount = flyingCount;
    }

    public boolean isIterative() {
        return isIterative;
    }

    public void setIterative(boolean iterative) {
        isIterative = iterative;
    }

    public AffinityLevel getAffinityLevel() {
        return affinityLevel;
    }

    public void setAffinityLevel(AffinityLevel affinityLevel) {
        this.affinityLevel = affinityLevel;
    }

    @Override
    public String toString() {
        return "CycleGroupMeta{"
            + "iterationCount=" + iterationCount
            + ", flyingCount=" + flyingCount
            + ", isIterative=" + isIterative
            + '}';
    }
}
