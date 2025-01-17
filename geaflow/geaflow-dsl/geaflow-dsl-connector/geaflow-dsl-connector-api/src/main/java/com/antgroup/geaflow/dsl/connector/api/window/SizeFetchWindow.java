package com.antgroup.geaflow.dsl.connector.api.window;

import com.antgroup.geaflow.api.window.WindowType;

/**
 * size window.
 */
public class SizeFetchWindow extends AbstractFetchWindow {

    private final long size;

    public SizeFetchWindow(long windowId, long size) {
        super(windowId);
        this.size = size;
    }

    @Override
    public long windowSize() {
        return size;
    }

    @Override
    public WindowType getType() {
        return WindowType.SIZE_TUMBLING_WINDOW;
    }
}
