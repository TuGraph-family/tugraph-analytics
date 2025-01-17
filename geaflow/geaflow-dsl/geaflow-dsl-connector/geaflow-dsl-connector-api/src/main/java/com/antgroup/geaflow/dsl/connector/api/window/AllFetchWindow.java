package com.antgroup.geaflow.dsl.connector.api.window;

import com.antgroup.geaflow.api.window.WindowType;

/**
 * Fetch all.
 */
public class AllFetchWindow extends AbstractFetchWindow {

    public AllFetchWindow(long windowId) {
        super(windowId);
    }

    @Override
    public long windowSize() {
        return -1;
    }

    @Override
    public WindowType getType() {
        return WindowType.ALL_WINDOW;
    }
}
