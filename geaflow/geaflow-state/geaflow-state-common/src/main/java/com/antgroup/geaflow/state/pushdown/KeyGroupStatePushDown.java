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

package com.antgroup.geaflow.state.pushdown;

import com.antgroup.geaflow.utils.keygroup.KeyGroup;

public class KeyGroupStatePushDown<K, T, R> extends StatePushDown<K, T, R> {

    private KeyGroup keyGroup;

    private KeyGroupStatePushDown() {}

    public static KeyGroupStatePushDown of() {
        return new KeyGroupStatePushDown();
    }

    public static KeyGroupStatePushDown of(KeyGroup keyGroup) {
        return new KeyGroupStatePushDown().withKeyGroup(keyGroup);
    }

    public static KeyGroupStatePushDown of(StatePushDown statePushDown) {
        KeyGroupStatePushDown pushDown = new KeyGroupStatePushDown();
        pushDown.filter = statePushDown.filter;
        pushDown.edgeLimit = statePushDown.edgeLimit;
        pushDown.filters = statePushDown.filters;
        pushDown.orderFields = statePushDown.orderFields;
        pushDown.projector = statePushDown.projector;
        pushDown.pushdownType = statePushDown.pushdownType;
        return pushDown;
    }

    public KeyGroup getKeyGroup() {
        return keyGroup;
    }

    public KeyGroupStatePushDown<K, T, R> withKeyGroup(KeyGroup keyGroup) {
        this.keyGroup = keyGroup;
        return this;
    }
}
