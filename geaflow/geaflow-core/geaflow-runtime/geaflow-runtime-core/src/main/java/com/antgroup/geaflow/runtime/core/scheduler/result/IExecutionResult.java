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

package com.antgroup.geaflow.runtime.core.scheduler.result;

public interface IExecutionResult<R, E> {

    /**
     * Get execution result.
     * @return
     */
    R getResult();

    /**
     * Returns execution error.
     */
    E getError();

    /**
     * Check whether the execution is successful.
     *   if true, then can get result from {@link #getResult()}
     *   otherwise, then can get result from {@link #getError()}
     */
    boolean isSuccess();

}
