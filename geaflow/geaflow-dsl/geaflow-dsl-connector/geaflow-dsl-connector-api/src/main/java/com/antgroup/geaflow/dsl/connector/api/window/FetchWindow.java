package com.antgroup.geaflow.dsl.connector.api.window;

import com.antgroup.geaflow.api.window.WindowType;

/**
 * Interface for the table source fetch records.
 */
public interface FetchWindow {

    /**
     * Return the window id.
     */
    long windowId();

    /**
     * Return the window size.
     */
    long windowSize();

    /**
     * Return the window type.
     */
    WindowType getType();

}
