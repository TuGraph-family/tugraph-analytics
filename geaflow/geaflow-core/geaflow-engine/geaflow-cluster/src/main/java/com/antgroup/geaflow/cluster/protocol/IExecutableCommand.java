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

package com.antgroup.geaflow.cluster.protocol;

import com.antgroup.geaflow.cluster.task.ITaskContext;

/**
 * A executable command is the event of executable command.
 */
public interface IExecutableCommand extends ICommand {

    /**
     * Define compute logic process for the corresponding command.
     */
    void execute(ITaskContext taskContext);

    /**
     * Interrupt current running command.
     */
    void interrupt();
}
