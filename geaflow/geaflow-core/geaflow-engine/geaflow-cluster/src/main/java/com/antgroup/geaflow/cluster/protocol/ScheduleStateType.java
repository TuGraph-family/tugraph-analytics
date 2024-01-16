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

import java.io.Serializable;

public enum ScheduleStateType implements Serializable {

    /**
     * Start state.
     */
    START,

    /**
     * Init state.
     */
    INIT,

    /**
     * Init graph and execute first iteration state.
     */
    ITERATION_INIT,

    /**
     * Execute state.
     */
    EXECUTE_COMPUTE,

    /**
     * Finish iteration state.
     */
    ITERATION_FINISH,

    /**
     * Clean cycle.
     */
    CLEAN_CYCLE,

    /**
     * Rollback state.
     */
    ROLLBACK,

    /**
     * Compose state.
     */
    COMPOSE,

    /**
     * End state.
     */
    END,

}
