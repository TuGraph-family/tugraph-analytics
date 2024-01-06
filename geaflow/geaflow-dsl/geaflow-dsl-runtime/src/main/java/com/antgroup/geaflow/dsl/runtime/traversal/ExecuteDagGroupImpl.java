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

package com.antgroup.geaflow.dsl.runtime.traversal;

import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.StepRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.IdOnlyVertex;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.VertexRecord;
import com.antgroup.geaflow.dsl.runtime.traversal.message.EODMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.IPathMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageBox;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.operator.StepOperator;
import com.antgroup.geaflow.dsl.runtime.traversal.path.EmptyTreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExecuteDagGroupImpl implements ExecuteDagGroup {

    private final DagTopologyGroup dagGroup;

    private TraversalRuntimeContext context;

    private final List<MessageBox> broadcastMessages = new ArrayList<>();

    public ExecuteDagGroupImpl(DagTopologyGroup dagGroup) {
        this.dagGroup = dagGroup;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        context.setTopology(dagGroup);
        for (DagTopology dagTopology : dagGroup.getAllDagTopology()) {
            dagTopology.getEntryOperator().open(context);
        }
        this.context = context;
    }

    @Override
    public void execute(Object vertexId, long... receiverOpIds) {
        RowVertex vertex = IdOnlyVertex.of(vertexId);
        for (long receiverOpId : receiverOpIds) {
            StepOperator<StepRecord, StepRecord> operator = dagGroup.getOperator(receiverOpId);
            // set current process operator id.
            context.setCurrentOpId(operator.getId());
            context.setVertex(vertex);
            ParameterRequestMessage requestMessage = context.getMessage(MessageType.PARAMETER_REQUEST);
            if (requestMessage != null && !requestMessage.isEmpty()) { // execute for each request id.
                requestMessage.forEach(request -> doExecute(operator, vertex, request));
            } else { // execute for the case without request message.
                doExecute(operator, vertex, null);
            }
        }
    }

    private void doExecute(StepOperator<StepRecord, StepRecord> operator,
                           RowVertex vertex,
                           ParameterRequest request) {
        // set current request
        context.setRequest(request);
        context.setCurrentOpId(operator.getId());
        context.setVertex(vertex);
        IPathMessage pathMessage = context.getMessage(MessageType.PATH);
        ITreePath treePath = pathMessage == null ? EmptyTreePath.INSTANCE : (ITreePath) pathMessage;

        operator.process(VertexRecord.of(vertex, treePath));
    }

    public void finishIteration(long iterationId) {
        StepOperator<StepRecord, StepRecord> mainOp = dagGroup.getMainDag().getEntryOperator();
        if (iterationId == 1) {
            mainOp.process(EndOfData.of(mainOp.getId()));
        } else {
            // process broadcast message after other normal message has processed.
            for (MessageBox messageBox : broadcastMessages) {
                long[] receiverOpIds = messageBox.getReceiverIds();
                for (long receiverOpId : receiverOpIds) {
                    StepOperator<StepRecord, StepRecord> operator = dagGroup.getOperator(receiverOpId);
                    EODMessage eodMessage = messageBox.getMessage(receiverOpId, MessageType.EOD);
                    if (eodMessage != null) {
                        for (EndOfData endOfData : eodMessage.getEodData()) {
                            operator.process(endOfData);
                        }
                    }
                }
            }
            broadcastMessages.clear();
        }
    }

    @Override
    public void processBroadcast(MessageBox messageBox) {
        broadcastMessages.add(messageBox);
    }

    @Override
    public void close() {
        Collection<StepOperator<StepRecord, StepRecord>> operators = dagGroup.getAllOperators();
        for (StepOperator<StepRecord, StepRecord> operator : operators) {
            operator.close();
        }
    }

    @Override
    public long getEntryOpId() {
        return dagGroup.getMainDag().getEntryOpId();
    }

    @Override
    public DagTopology getMainDag() {
        return dagGroup.getMainDag();
    }

    @Override
    public int getMaxIterationCount() {
        return dagGroup.getIterationCount(1, dagGroup.getMainDag().getEntryOperator());
    }
}
