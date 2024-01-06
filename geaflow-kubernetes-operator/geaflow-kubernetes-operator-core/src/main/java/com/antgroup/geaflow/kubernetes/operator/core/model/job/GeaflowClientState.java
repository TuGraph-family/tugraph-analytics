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

package com.antgroup.geaflow.kubernetes.operator.core.model.job;


public enum GeaflowClientState {

    /**
     * Client pod is not deployed yet.
     */
    NOT_DEPLOYED,

    /**
     * Client pod is already created.
     */
    DEPLOYED_NOT_READY,

    /**
     * Client pod is running, and the main class is still running.
     */
    RUNNING,

    /**
     * Client pod is already deleted.
     */
    EXITED,

    /**
     * Client pod is in error state such as ErrPullImage.
     */
    ERROR

}
