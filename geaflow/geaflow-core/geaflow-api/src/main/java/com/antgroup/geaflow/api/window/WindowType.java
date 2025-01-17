package com.antgroup.geaflow.api.window;

public enum WindowType {
    ALL_WINDOW, // all data
    FIXED_TIME_TUMBLING_WINDOW, // window with time unit
    SIZE_TUMBLING_WINDOW, // window with size unit
    CUSTOM
}
