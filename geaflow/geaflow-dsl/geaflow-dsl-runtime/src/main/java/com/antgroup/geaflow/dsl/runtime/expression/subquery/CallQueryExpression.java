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

package com.antgroup.geaflow.dsl.runtime.expression.subquery;

import com.antgroup.geaflow.common.type.IType;
import com.antgroup.geaflow.common.utils.ArrayUtil;
import com.antgroup.geaflow.dsl.common.data.Path;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.function.FunctionContext;
import com.antgroup.geaflow.dsl.common.types.VertexType;
import com.antgroup.geaflow.dsl.runtime.expression.AbstractExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.CallRequestId;
import com.antgroup.geaflow.dsl.runtime.traversal.data.EndOfData;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import com.antgroup.geaflow.dsl.runtime.traversal.data.SingleValue;
import com.antgroup.geaflow.dsl.runtime.traversal.message.EODMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.MessageType;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ParameterRequestMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessage;
import com.antgroup.geaflow.dsl.runtime.traversal.message.ReturnMessageImpl.ReturnKey;
import com.antgroup.geaflow.dsl.runtime.traversal.path.ITreePath;
import com.antgroup.geaflow.dsl.runtime.traversal.path.TreePaths;
import java.util.Collections;
import java.util.List;

public class CallQueryExpression extends AbstractExpression implements ICallQuery {

    private final String queryName;

    /**
     * The operator id of the start operator for the sub query dag.
     */
    private final long queryId;

    private final int startVertexIndex;

    private final VertexType startVertexType;

    private final IType<?> returnType;

    private TraversalRuntimeContext context;

    private CallState callState;

    private final int[] refParentPathIndices;

    private final Object defaultAggValue;

    public CallQueryExpression(String queryName, long queryId,
                               int startVertexIndex, VertexType startVertexType,
                               IType<?> returnType, int[] refParentPathIndices,
                               Object defaultAggValue) {
        this.queryName = queryName;
        this.queryId = queryId;
        this.startVertexIndex = startVertexIndex;
        this.startVertexType = startVertexType;
        this.returnType = returnType;
        this.refParentPathIndices = refParentPathIndices;
        this.defaultAggValue = defaultAggValue;
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        this.context = context;
        this.open(FunctionContext.of(context.getConfig()));
        this.callState = CallState.INIT;
    }

    @Override
    public void setCallState(CallState callState) {
        this.callState = callState;
    }

    @Override
    public CallState getCallState() {
        return callState;
    }

    @Override
    public Object evaluate(Row row) {
        RowVertex startVertex = (RowVertex) row.getField(startVertexIndex, startVertexType);
        Path path = (Path) row;
        if (callState == CallState.WAITING) {
            ParameterRequestMessage requestMessage = new ParameterRequestMessage();
            Row parameter = context.getParameters();
            long uniquePathId = context.createUniqueId(path.getId());
            CallRequestId callRequestId = new CallRequestId(uniquePathId, context.getCurrentOpId(),
                startVertex.getId());
            ParameterRequest request = new ParameterRequest(callRequestId, startVertex.getId(), parameter);

            requestMessage.addRequest(request);
            // send request message to sub query plan's start query operator id.
            context.sendMessage(startVertex.getId(), requestMessage, queryId);
            Path pathMessage = ((Path) row).subPath(refParentPathIndices);
            ITreePath treePath = TreePaths.createTreePath(Collections.singletonList(pathMessage));
            treePath.setRequestIdForTree(callRequestId);
            context.sendMessage(startVertex.getId(), treePath, queryId);
            return null;
        } else if (callState == CallState.RETURNING) {
            ReturnMessage returnMessage = context.getMessage(MessageType.RETURN_VALUE);
            long uniquePathId = context.createUniqueId(path.getId());
            ReturnKey returnKey = new ReturnKey(uniquePathId, queryId);
            SingleValue singleValue = returnMessage.getValue(returnKey);
            if (singleValue == null) {
                return defaultAggValue;
            }
            return singleValue.getValue(returnType);
        }
        throw new IllegalArgumentException("Illegal call state: " + callState + " for evaluate() method");
    }

    public void sendEod() {
        EndOfData eod = EndOfData.of(context.getCurrentOpId(), context.getCurrentOpId());
        context.broadcast(EODMessage.of(eod), queryId);
    }

    @Override
    public void finishCall() {

    }

    public String getQueryName() {
        return queryName;
    }

    @Override
    public String showExpression() {
        return "Call(" + queryName + ")";
    }

    @Override
    public IType<?> getOutputType() {
        return returnType;
    }

    @Override
    public List<Expression> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public List<Integer> getRefPathFieldIndices() {
        return ArrayUtil.toList(refParentPathIndices);
    }

    public enum CallState {
        INIT,
        CALLING,
        WAITING,
        RETURNING,
        FINISH
    }
}
