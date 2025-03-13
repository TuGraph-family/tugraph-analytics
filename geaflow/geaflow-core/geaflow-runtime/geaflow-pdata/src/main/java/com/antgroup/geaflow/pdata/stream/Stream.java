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

package com.antgroup.geaflow.pdata.stream;

import com.antgroup.geaflow.api.pdata.base.PData;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.operator.OpArgs;
import com.antgroup.geaflow.operator.Operator;
import com.antgroup.geaflow.operator.base.AbstractOperator;
import com.antgroup.geaflow.partitioner.IPartitioner;
import com.antgroup.geaflow.partitioner.impl.ForwardPartitioner;
import com.antgroup.geaflow.pipeline.context.IPipelineContext;

import java.io.Serializable;
import java.util.Map;

public abstract class Stream<T> implements PData, Serializable {

    private int id;
    protected int parallelism = 1;

    protected Stream input;

    protected OpArgs opArgs;
    protected Operator operator;
    protected IPipelineContext context;
    protected IEncoder<T> encoder;

    protected Stream() {

    }

    public Stream(IPipelineContext context) {
        this.id = context.generateId();
        this.context = context;
    }

    public Stream(IPipelineContext context, Operator operator) {
        this(context);
        this.operator = operator;
        this.opArgs = ((AbstractOperator)operator).getOpArgs();
        this.opArgs.setOpId(this.id);
    }

    public Stream(Stream dataStream, Operator operator) {
        this(dataStream.getContext(), operator);
        this.input = dataStream;
        this.parallelism = input.getParallelism();
        this.opArgs.setParallelism(parallelism);
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    protected void updateId() {
        this.id = context.generateId();
    }

    public Operator getOperator() {
        this.opArgs.setOpId(this.id);
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
        if (input != null) {
            this.opArgs.setParallelism(input.getParallelism());
        }
    }

    public IPipelineContext getContext() {
        return context;
    }

    public <S extends Stream<T>> S getInput() {
        return (S) this.input;
    }

    public IPartitioner getPartition() {
        return new ForwardPartitioner(this.getId());
    }

    public int getParallelism() {
        return this.parallelism;
    }

    protected void setParallelism(int parallelism) {
        this.parallelism = parallelism;
        this.opArgs.setParallelism(parallelism);
    }

    protected void setName(String name) {
        this.opArgs.setOpName(name);
    }

    public void setConfig(Map<String, String> config) {
        this.opArgs.setConfig(config);
    }

    public void setConfig(String key, String value) {
        this.opArgs.getConfig().put(key, value);
    }

    public TransformType getTransformType() {
        return TransformType.StreamTransform;
    }

    public Stream<T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }

    public IEncoder<T> getEncoder() {
        return this.encoder;
    }

}
