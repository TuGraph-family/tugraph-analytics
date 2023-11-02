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


public enum JobState {

    /**
     * Job is Not submitted.
     */
    INIT,

    /**
     * Job is already submitted, and the main class is still running.
     */
    SUBMITTED,

    /**
     * Job is running, and the master service has been already created.
     */
    RUNNING,

    /**
     * Job is suspended.
     */
    SUSPENDED,

    /**
     * Job is updated, and need to redeploy.
     */
    REDEPLOYING,

    /**
     * Job failed, and secondary resources of the job are deleted.
     */
    FAILED,

    /**
     * Job finished.
     */
    FINISHED

}
