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

package com.antgroup.geaflow.state.graph.encoder;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BytesEncoderRepo {

    private static final Map<Byte, IBytesEncoder> ENCODER_MAP = new ConcurrentHashMap<>();

    public static void register(IBytesEncoder encoder) {
        ENCODER_MAP.put(encoder.getMyMagicNumber(), encoder);
    }

    public static IBytesEncoder get(byte myMagicNumber) {
        return Preconditions.checkNotNull(ENCODER_MAP.get(myMagicNumber),
            "not found encoder " + myMagicNumber);
    }

    static {
        register(new DefaultBytesEncoder());
    }
}
