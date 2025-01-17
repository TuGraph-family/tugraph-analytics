package com.antgroup.geaflow.api.window.impl;

import com.antgroup.geaflow.api.window.ITumblingWindow;
import com.antgroup.geaflow.api.window.WindowType;

public class FixedTimeTumblingWindow<T> implements ITumblingWindow<T> {

    private final long timeWindowInSecond;
    private long windowId;

    public FixedTimeTumblingWindow(long timeWindowInSecond) {
        this.timeWindowInSecond = timeWindowInSecond;
    }

    public long getTimeWindowSize() {
        return timeWindowInSecond;
    }

    @Override
    public long windowId() {
        return windowId;
    }

    @Override
    public void initWindow(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public long assignWindow(T value) {
        return windowId;
    }

    @Override
    public WindowType getType() {
        return WindowType.FIXED_TIME_TUMBLING_WINDOW;
    }
}
