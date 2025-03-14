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

package com.antgroup.geaflow.memory.array;

import com.antgroup.geaflow.memory.MemoryViewReference;

public class ByteArrayBuilder {

    public static ByteArray of(MemoryViewReference viewRef, int offset, int len) {
        return new ByteArrayWithMemoryView(viewRef, offset, len);
    }

    public static ByteArray of(byte[] bytes) {
        return new DefaultByteArray(bytes);
    }

    public static ByteArray of(byte[] bytes, int offset, int len) {
        return new DefaultByteArray(bytes, offset, len);
    }
}
