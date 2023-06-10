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

package com.antgroup.geaflow.common.shuffle;

public enum DataExchangeMode {

    /**
     * The data exchange is streamed, sender and receiver are online at the same time.
     */
    PIPELINE,

    /**
     * The data exchange is decoupled. The sender first produces its entire result and
     * finishes. After that, the receiver is started and may consume the data.
     */
    BATCH

}
