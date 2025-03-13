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

package com.antgroup.geaflow.runtime.core.worker;

import com.antgroup.geaflow.cluster.fetcher.PrefetchMessageBuffer;
import com.antgroup.geaflow.shuffle.message.SliceId;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PrefetchCallbackHandler {

    private static volatile PrefetchCallbackHandler INSTANCE;

    private final Map<SliceId, PrefetchCallback> taskId2callback = new ConcurrentHashMap<>();

    public static PrefetchCallbackHandler getInstance() {
        if (INSTANCE == null) {
            synchronized (PrefetchCallbackHandler.class) {
                if (INSTANCE == null) {
                    INSTANCE = new PrefetchCallbackHandler();
                }
            }
        }
        return INSTANCE;
    }


    public void registerTaskEventCallback(SliceId sliceId, PrefetchCallback prefetchCallback) {
        this.taskId2callback.put(sliceId, prefetchCallback);
    }

    public PrefetchCallback removeTaskEventCallback(SliceId sliceId) {
        return this.taskId2callback.remove(sliceId);
    }

    public static class PrefetchCallback {

        private final PrefetchMessageBuffer<?> buffer;

        public PrefetchCallback(PrefetchMessageBuffer<?> buffer) {
            this.buffer = buffer;
        }

        public void execute() {
            this.buffer.waitUtilFinish();
        }

    }

}
