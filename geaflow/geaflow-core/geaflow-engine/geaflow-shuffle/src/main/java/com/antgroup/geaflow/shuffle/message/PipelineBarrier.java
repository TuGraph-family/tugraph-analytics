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

package com.antgroup.geaflow.shuffle.message;

import java.util.Objects;

public class PipelineBarrier implements PipelineEvent {

    // Input edge id.
    private final int edgeId;

    // Iteration id of cycle.
    private final long batchId;

    // If of source task that send the event from.
    private final int sourceTaskIndex;

    // Id of target task that send the event to.
    private int targetTaskIndex;

    // Message count that current event involved.
    private long count;

    // Flag that denote source task is finished after the current event.
    private boolean finish;

    public PipelineBarrier(long batchId, int edgeId, int sourceTaskIndex) {
        this.edgeId = edgeId;
        this.batchId = batchId;
        this.sourceTaskIndex = sourceTaskIndex;
    }

    public PipelineBarrier(long batchId, int edgeId, int sourceTaskIndex, int targetTaskId,
                           long count) {
        this.edgeId = edgeId;
        this.batchId = batchId;
        this.sourceTaskIndex = sourceTaskIndex;
        this.targetTaskIndex = targetTaskId;
        this.count = count;
        this.finish = false;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }

    public boolean isFinish() {
        return finish;
    }

    @Override
    public long getWindowId() {
        return batchId;
    }

    public int getSourceTaskIndex() {
        return sourceTaskIndex;
    }

    public int getTargetTaskIndex() {
        return targetTaskIndex;
    }

    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipelineBarrier that = (PipelineBarrier) o;
        return edgeId == that.edgeId && batchId == that.batchId
            && sourceTaskIndex == that.sourceTaskIndex && targetTaskIndex == that.targetTaskIndex
            && count == that.count && finish == that.finish;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeId, batchId, sourceTaskIndex, targetTaskIndex, count, finish);
    }

    @Override
    public String toString() {
        return "PipelineBarrier{" + "edgeId=" + edgeId + ", batchId=" + batchId
            + ", sourceTaskIndex=" + sourceTaskIndex + ", targetTaskIndex=" + targetTaskIndex
            + ", count=" + count + ", finish=" + finish + '}';
    }

}
