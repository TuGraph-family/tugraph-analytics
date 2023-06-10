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
import com.antgroup.geaflow.dsl.runtime.expression.AbstractExpression;
import com.antgroup.geaflow.dsl.runtime.expression.Expression;
import com.antgroup.geaflow.dsl.runtime.expression.subquery.CallQueryExpression.CallState;
import com.antgroup.geaflow.dsl.runtime.traversal.TraversalRuntimeContext;
import com.antgroup.geaflow.dsl.runtime.traversal.data.ParameterRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CallQueryProxy extends AbstractExpression implements ICallQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallQueryProxy.class);

    private final CallQueryExpression[] queryCalls;

    private final Expression rewriteExpression;

    private final PlaceHolderExpression[] placeHolderExpressions;

    private CallState callState;

    private Map<ParameterRequest, Map<Object, List<Path>>> stashPaths;

    private TraversalRuntimeContext context;

    private CallQueryProxy(CallQueryExpression[] queryCalls,
                           Expression rewriteExpression) {
        this.queryCalls = Objects.requireNonNull(queryCalls);
        this.rewriteExpression = Objects.requireNonNull(rewriteExpression);
        this.placeHolderExpressions = rewriteExpression.collect(exp -> exp instanceof PlaceHolderExpression)
            .toArray(new PlaceHolderExpression[]{});
    }

    public static Expression from(Expression expression) {
        List<CallQueryExpression> calls =
            ArrayUtil.castList(expression.collect(exp -> exp instanceof CallQueryExpression));
        if (calls.isEmpty()) {
            return expression;
        }
        Map<CallQueryExpression, Integer> callIndexMap = new HashMap<>();
        for (int i = 0; i < calls.size(); i++) {
            callIndexMap.put(calls.get(i), i);
        }
        CallQueryExpression[] callArrays = calls.toArray(new CallQueryExpression[]{});
        Expression rewriteExpression = expression.replace(exp -> {
            if (exp instanceof CallQueryExpression) {
                int index = callIndexMap.get(exp);
                return new PlaceHolderExpression(callArrays, index);
            }
            return exp;
        });
        return new CallQueryProxy(callArrays, rewriteExpression);
    }

    @Override
    public void open(TraversalRuntimeContext context) {
        for (CallQueryExpression call : queryCalls) {
            call.open(context);
        }
        this.context = context;
        this.stashPaths = new HashMap<>();
        this.callState = CallState.INIT;
    }

    @Override
    public Object evaluate(Row row) {
        if (callState == CallState.CALLING) {
            // stash paths.
            stashPaths.computeIfAbsent(context.getRequest(), k -> new HashMap<>())
                .computeIfAbsent(context.getVertex().getId(), id -> new ArrayList<>())
                .add(((Path) row).copy());
            return null;
        }

        if (callState == CallState.RETURNING) {
            Object[] callResults = new Object[queryCalls.length];
            for (int i = 0; i < queryCalls.length; i++) {
                callResults[i] = queryCalls[i].evaluate(row);
            }
            for (PlaceHolderExpression placeHolderExpression : placeHolderExpressions) {
                placeHolderExpression.setResults(callResults);
            }
            return rewriteExpression.evaluate(row);
        }
        throw new IllegalArgumentException("Illegal call state: " + callState + " for evaluate() method");
    }

    @Override
    public String showExpression() {
        return rewriteExpression.showExpression();
    }

    @Override
    public IType<?> getOutputType() {
        return rewriteExpression.getOutputType();
    }

    @Override
    public List<Expression> getInputs() {
        return rewriteExpression.getInputs();
    }

    @Override
    public Expression copy(List<Expression> inputs) {
        return new CallQueryProxy(queryCalls, rewriteExpression.copy(inputs));
    }

    @Override
    public void setCallState(CallState callState) {
        this.callState = callState;
        for (CallQueryExpression call : queryCalls) {
            call.setCallState(callState);
        }
    }

    @Override
    public CallState getCallState() {
        return callState;
    }

    public CallQueryExpression[] getQueryCalls() {
        return queryCalls;
    }

    public List<String> getSubQueryNames() {
        List<String> names = new ArrayList<>();
        for (CallQueryExpression queryCall : queryCalls) {
            names.add(queryCall.getQueryName());
        }
        return names;
    }

    @Override
    public void finishCall() {
        if (callState == CallState.WAITING) { // call sub query
            for (Map.Entry<ParameterRequest, Map<Object, List<Path>>> entry : stashPaths.entrySet()) {
                ParameterRequest request = entry.getKey();
                Map<Object, List<Path>> vertexPaths = entry.getValue();
                for (Map.Entry<Object, List<Path>> vertexPathEntry : vertexPaths.entrySet()) {
                    List<Path> paths = vertexPathEntry.getValue();
                    context.setRequest(request);
                    for (Path path : paths) {
                        for (CallQueryExpression queryCall : queryCalls) {
                            queryCall.evaluate(path);
                        }
                    }
                }
            }
            // send eod to the sub query after call finish.
            for (CallQueryExpression queryCall : queryCalls) {
                queryCall.sendEod();
            }
            stashPaths.clear();
        }

        for (ICallQuery queryCall : queryCalls) {
            queryCall.finishCall();
        }
    }

    private static class PlaceHolderExpression extends AbstractExpression {

        private final Expression[] expressions;

        private final int placeHolderIndex;

        private Object[] results;

        public PlaceHolderExpression(Expression[] expressions, int placeHolderIndex) {
            this.expressions = expressions;
            this.placeHolderIndex = placeHolderIndex;
        }

        public void setResults(Object[] results) {
            this.results = results;
        }

        @Override
        public Object evaluate(Row row) {
            return results[placeHolderIndex];
        }

        @Override
        public String showExpression() {
            return expressions[placeHolderIndex].showExpression();
        }

        @Override
        public IType<?> getOutputType() {
            return expressions[placeHolderIndex].getOutputType();
        }

        @Override
        public List<Expression> getInputs() {
            return expressions[placeHolderIndex].getInputs();
        }

        @Override
        public Expression copy(List<Expression> inputs) {
            expressions[placeHolderIndex] = expressions[placeHolderIndex].copy(inputs);
            return new PlaceHolderExpression(expressions, placeHolderIndex);
        }
    }
}
