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

package com.antgroup.geaflow.processor.builder;

import com.antgroup.geaflow.common.errorcode.RuntimeErrors;
import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import com.antgroup.geaflow.operator.OpArgs.OpType;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.operator.base.window.OneInputOperator;
import com.antgroup.geaflow.operator.base.window.TwoInputOperator;
import com.antgroup.geaflow.operator.impl.graph.algo.vc.AbstractGraphVertexCentricOp;
import com.antgroup.geaflow.operator.impl.io.WindowSourceOperator;
import com.antgroup.geaflow.processor.Processor;
import com.antgroup.geaflow.processor.impl.graph.GraphVertexCentricProcessor;
import com.antgroup.geaflow.processor.impl.io.SourceProcessor;
import com.antgroup.geaflow.processor.impl.window.OneInputProcessor;
import com.antgroup.geaflow.processor.impl.window.TwoInputProcessor;
import com.google.common.base.Preconditions;

public class ProcessorBuilder implements IProcessorBuilder {

    @Override
    public Processor buildProcessor(Operator operator) {
        Processor processor = null;
        OpType type = ((AbstractOperator) operator).getOpArgs().getOpType();
        String msg = String.format("operator %s type is null", operator);
        Preconditions.checkArgument(type != null, msg);
        switch (type) {
            case SINGLE_WINDOW_SOURCE:
            case MULTI_WINDOW_SOURCE:
                processor = new SourceProcessor((WindowSourceOperator) operator);
                break;
            case ONE_INPUT:
                processor = new OneInputProcessor((OneInputOperator) operator);
                break;
            case TWO_INPUT:
                processor = new TwoInputProcessor((TwoInputOperator) operator);
                break;
            case GRAPH_SOURCE:
                break;
            case VERTEX_CENTRIC_COMPUTE:
            case VERTEX_CENTRIC_COMPUTE_WITH_AGG:
            case VERTEX_CENTRIC_TRAVERSAL:
            case INC_VERTEX_CENTRIC_COMPUTE:
            case INC_VERTEX_CENTRIC_TRAVERSAL:
                processor = new GraphVertexCentricProcessor((AbstractGraphVertexCentricOp) operator);
                break;
            default:
                throw new GeaflowRuntimeException(RuntimeErrors.INST.operatorTypeNotSupportError(type.name()));
        }

        return processor;
    }
}

