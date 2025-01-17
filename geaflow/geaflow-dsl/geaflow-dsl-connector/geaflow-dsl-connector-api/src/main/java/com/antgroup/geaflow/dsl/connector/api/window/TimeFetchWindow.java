package com.antgroup.geaflow.dsl.connector.api.window;

import com.antgroup.geaflow.api.window.WindowType;

/**
 * Time window.
 */
public class TimeFetchWindow extends AbstractFetchWindow {

    private final long windowSizeInSecond;

    public TimeFetchWindow(long windowId, long windowSizeInSecond) {
        super(windowId);
        this.windowSizeInSecond = windowSizeInSecond;
    }

    // include
    public long getStartWindowTime(long startTime) {
        return startTime + windowId * windowSizeInSecond * 1000;
    }

    // exclude
    public long getEndWindowTime(long startTime) {
        return startTime + (windowId + 1) * windowSizeInSecond * 1000;
    }

    @Override
    public long windowSize() {
        return windowSizeInSecond;
    }

    @Override
    public WindowType getType() {
        return WindowType.FIXED_TIME_TUMBLING_WINDOW;
    }
}
