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

package com.antgroup.geaflow.dsl.runtime.traversal.collector;

import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.common.data.StepRecord.StepRecordType;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.CallRequestId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import com.antgroup.geaflow.dsl.runtime.traversal.message.EODMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl.ReturnKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepReturnCollector implements StepCollector<StepRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StepReturnCollector.class);

    private final TraversalRuntimeContext context;

    private final long queryId;

    public StepReturnCollector(TraversalRuntimeContext context, long currentOpId) {
        this.context = context;
        this.queryId = context.getTopology().getDagTopology(currentOpId).getEntryOpId();
    }

    @Override
    public void collect(StepRecord record) {
        StepRecordType recordType = record.getType();
        if (recordType == StepRecordType.SINGLE_VALUE) {
            ParameterRequest request = context.getRequest();
            CallRequestId callRequestId = (CallRequestId) request.getRequestId();
            long callOpId = callRequestId.getCallOpId();
            sendReturnValue(callOpId, callRequestId.getPathId(), request.getVertexId(), (SingleValue) record);
        } else if (recordType == StepRecordType.EOD) {
            Iterable<CallRequestId> callRequestIds = context.takeCallRequestIds();
            // Send default return value: 'null' to the caller as the request may filter by the middle operator
            // and cannot reach to the return operator. If the request can reach the return operator, the return value
            // will update the null value.
            for (CallRequestId callRequestId : callRequestIds) {
                long callOpId = callRequestId.getCallOpId();
                Object startVertexId = callRequestId.getVertexId();
                sendReturnValue(callOpId, callRequestId.getPathId(), startVertexId, null);
            }
            // send eod to the caller after call return.
            EndOfData eod = (EndOfData) record;
            assert eod.getCallOpId() >= 0 : "Illegal caller op id: " + eod.getCallOpId();
            long callerId = eod.getCallOpId();
            eod = EndOfData.of(-1, eod.getSenderId());
            context.broadcast(EODMessage.of(eod), callerId);
        }
    }

    private void sendReturnValue(long callerOpId, long pathId, Object startVertexId, SingleValue value) {
        // send back the result value.
        ReturnMessage returnMessage = new ReturnMessageImpl();
        returnMessage.putValue(new ReturnKey(pathId, queryId), value);
        context.sendMessage(startVertexId, returnMessage, callerOpId);
    }
}
