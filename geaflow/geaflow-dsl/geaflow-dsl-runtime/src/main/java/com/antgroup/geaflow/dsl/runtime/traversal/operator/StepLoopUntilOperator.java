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

package com.antgroup.geaflow.dsl.runtime.traversal.operator;

import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.function.graph.StepBoolFunction;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.collector.StepJumpCollector;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.StepRecordWithPath;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import java.util.List;

public class StepLoopUntilOperator extends AbstractStepOperator<StepBoolFunction,
    StepRecordWithPath, StepRecordWithPath> {

    private final long loopStartOpId;
    private final long loopBodyOpId;
    private StepCollector<StepRecord> loopStartCollector;
    private int loopCounter;

    private final int minLoopCount;
    private final int maxLoopCount;

    private final int loopStartPathFieldCount;
    private final int loopBodyPathFieldCount;

    private int[] pathIndices;

    public StepLoopUntilOperator(long id, long loopStartOpId,
                                 long loopBodyOpId, StepBoolFunction function,
                                 int minLoopCount, int maxLoopCount,
                                 int loopStartPathFieldCount, int loopBodyPathFieldCount) {
        super(id, function);
        this.loopStartOpId = loopStartOpId;
        this.loopBodyOpId = loopBodyOpId;
        this.minLoopCount = minLoopCount;
        this.maxLoopCount = maxLoopCount == -1 ? Integer.MAX_VALUE : maxLoopCount;
        this.loopStartPathFieldCount = loopStartPathFieldCount;
        this.loopBodyPathFieldCount = loopBodyPathFieldCount;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        super.open(context);
        this.loopStartCollector = new StepJumpCollector(id, loopStartOpId, context);
        // When reach the loop-util, we have already looped 1 time, so the init loop counter should be 1.
        this.loopCounter = 1;
        this.pathIndices = new int[loopBodyPathFieldCount + loopStartPathFieldCount];
    }

    @Override
    public StepOperator<StepRecordWithPath, StepRecordWithPath> copyInternal() {
        return new StepLoopUntilOperator(id, loopStartOpId, loopBodyOpId, function, minLoopCount, maxLoopCount,
            loopStartPathFieldCount, loopBodyPathFieldCount);
    }

    @Override
    protected void processRecord(StepRecordWithPath record) {
        boolean fromZeroLoop = context.getInputOperatorId() != loopBodyOpId;
        StepRecordWithPath lastLoopPath = selectLastLoopPath(record, fromZeroLoop);
        if (fromZeroLoop) { // from the loop start (for loop 0)
            collect(lastLoopPath);
        } else { // from the loop body
            if (loopCounter >= minLoopCount) {
                collect(lastLoopPath);
            }
            if (loopCounter < maxLoopCount) {
                loopStartCollector.collect(record);
            }
        }
    }

    private StepRecordWithPath selectLastLoopPath(StepRecordWithPath record, boolean fromZeroLoop) {
        VertexRecord vertexRecord = (VertexRecord) record;
        if (fromZeroLoop) {
            return record.mapTreePath(treePath -> {
                ITreePath newTreePath = treePath;
                for (int i = 0; i < loopBodyPathFieldCount - 1; i++) {
                    newTreePath = newTreePath.extendTo((RowEdge) null);
                }
                return newTreePath.extendTo(vertexRecord.getVertex());
            });
        } else {
            for (int i = 0; i < loopStartPathFieldCount; i++) {
                pathIndices[i] = i;
            }
            for (int i = 0; i < loopBodyPathFieldCount; i++) {
                pathIndices[i + loopStartPathFieldCount] = loopStartPathFieldCount
                    + (loopCounter - 1) * loopBodyPathFieldCount + i;
            }
            return record.subPathSet(pathIndices);
        }
    }

    @Override
    protected void onReceiveAllEOD(long callerOpId, List<Long> receiveEodIds) {
        if (loopCounter < maxLoopCount) {
            // remove eod from the loop body.
            receiveEodIds.remove(loopBodyOpId);
            // send EOD to the loop start.
            loopStartCollector.collect(EndOfData.of(id));
        } else { // If no data in the loop or reach the max loop count,
            // it means the whole loop has finished. Just send EOD to the next.
            super.onReceiveAllEOD(callerOpId, receiveEodIds);
        }
        loopCounter++;
    }
}
