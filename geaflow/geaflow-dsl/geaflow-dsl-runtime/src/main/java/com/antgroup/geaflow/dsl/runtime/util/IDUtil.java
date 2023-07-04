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

package com.antgroup.geaflow.dsl.runtime.util;

public class IDUtil {
    /**
     * generate a unique id between all the tasks.
     * @param numTask The number of total tasks.
     * @param taskIndex The index for current task.
     * @param idInTask The unique id in the task.
     * @return A unique id between all the tasks.
     */
    public static long uniqueId(int numTask, int taskIndex, long idInTask) {
        return numTask * idInTask + taskIndex;
    }
}
