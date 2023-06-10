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

package com.antgroup.geaflow.cluster.task;

import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;

public class Task implements ITask {

    private ITaskContext context;
    private IExecutableCommand command;

    public Task() {
    }

    @Override
    public void init(ITaskContext taskContext) {
        this.context = taskContext;
        this.command = null;
    }

    @Override
    public void execute(IExecutableCommand command) {
        this.command = command;
        command.execute(this.context);
    }

    @Override
    public void interrupt() {
        if (command != null) {
            command.interrupt();
        }
    }

    @Override
    public void close() {
        this.context.close();
    }

}
