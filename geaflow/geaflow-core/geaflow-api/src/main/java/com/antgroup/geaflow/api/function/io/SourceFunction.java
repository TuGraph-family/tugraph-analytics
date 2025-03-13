/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.api.function.io;

import com.antgroup.geaflow.api.function.Function;
import com.antgroup.geaflow.api.window.IWindow;

public interface SourceFunction<T> extends Function {

    /**
     * Initialize source function.
     */
    void init(int parallel, int index);

    /**
     * Fetch data from source by window, and collect data by ctx.
     * @param window Used to split windows for source.
     * @param ctx The source context.
     */
    boolean fetch(IWindow<T> window, SourceContext<T> ctx) throws Exception;

    /**
     * Close source function.
     */
    void close();

    interface SourceContext<T> {

        /**
         * Partition element data.
         */
        boolean collect(T element) throws Exception;

    }

}
