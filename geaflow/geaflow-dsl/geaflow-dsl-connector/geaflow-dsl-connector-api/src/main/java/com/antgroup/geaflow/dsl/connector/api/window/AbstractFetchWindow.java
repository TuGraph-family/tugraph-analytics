package com.antgroup.geaflow.dsl.connector.api.window;

public abstract class AbstractFetchWindow implements FetchWindow {

    protected final long windowId;

    public AbstractFetchWindow(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public long windowId() {
        return windowId;
    }
}
