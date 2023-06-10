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

package com.antgroup.geaflow.env;


import com.antgroup.geaflow.env.ctx.EnvironmentContext;
import com.antgroup.geaflow.env.ctx.IEnvironmentContext;
import com.antgroup.geaflow.pipeline.Pipeline;

public abstract class Environment implements IEnvironment {

    protected Pipeline pipeline;
    protected IEnvironmentContext context;

    public Environment() {
        context = new EnvironmentContext();
    }

    public void addPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public IEnvironmentContext getEnvironmentContext() {
        return context;
    }
}
