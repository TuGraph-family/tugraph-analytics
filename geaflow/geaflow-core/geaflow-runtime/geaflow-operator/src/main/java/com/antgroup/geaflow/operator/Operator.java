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

package com.antgroup.geaflow.operator;

import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.collector.ICollector;
import java.io.Serializable;
import java.util.List;

public interface Operator extends Serializable {

    /**
     * Operator open.
     */
    void open(OpContext opContext);

    /**
     * Operator finish.
     */
    void finish();

    /**
     * Operator close.
     */
    void close();

    interface OpContext extends Serializable {

        /**
         * Returns the collectors.
         */
        List<ICollector> getCollectors();

        /**
         * Returns the runtime context.
         */
        RuntimeContext getRuntimeContext();
    }
}
