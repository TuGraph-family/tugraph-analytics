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
        pushDown.orderField = statePushDown.orderField;
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
