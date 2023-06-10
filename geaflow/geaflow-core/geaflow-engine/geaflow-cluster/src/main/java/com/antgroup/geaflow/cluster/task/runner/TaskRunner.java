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

package com.antgroup.geaflow.cluster.task.runner;

import com.antgroup.geaflow.cluster.protocol.ICommand;
import com.antgroup.geaflow.cluster.protocol.IExecutableCommand;
import com.antgroup.geaflow.cluster.task.Task;
import com.antgroup.geaflow.cluster.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskRunner extends AbstractTaskRunner<ICommand> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);
    private Task task;
    private ITaskRunnerContext taskRunnerContext;

    public TaskRunner() {
        super();
    }

    public void init(ITaskRunnerContext taskRunnerContext) {
        this.taskRunnerContext = taskRunnerContext;
    }

    @Override
    protected void process(ICommand command) {
        LOGGER.info("task Executor:{}", command);
        switch (command.getEventType()) {
            case CREATE_TASK:
                // Starting of task's life cycle.
                task = new Task();
                task.init(new TaskContext(taskRunnerContext));
                break;
            case DESTROY_TASK:
                // Ending of task's life cycle.
                task.close();
                task = null;
                break;
            default:
                // Execute task command.
                task.execute((IExecutableCommand) command);
                break;
        }
    }

    @Override
    public void interrupt() {
        super.interrupt();
        if (task != null) {
            task.interrupt();
        }
    }
}
