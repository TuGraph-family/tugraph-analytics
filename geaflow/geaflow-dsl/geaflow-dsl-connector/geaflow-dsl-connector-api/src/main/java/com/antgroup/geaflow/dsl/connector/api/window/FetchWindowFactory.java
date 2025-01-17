package com.antgroup.geaflow.dsl.connector.api.window;

import com.antgroup.geaflow.api.window.IWindow;
import com.antgroup.geaflow.api.window.impl.FixedTimeTumblingWindow;
import com.antgroup.geaflow.api.window.impl.SizeTumblingWindow;
import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;

/**
 * Convert to the fetch window from the common window.
 */
public class FetchWindowFactory {

    public static <T> FetchWindow createFetchWindow(IWindow<T> window) {
        switch (window.getType()) {
            case ALL_WINDOW:
                return new AllFetchWindow(window.windowId());
            case SIZE_TUMBLING_WINDOW:
                return new SizeFetchWindow(window.windowId(), ((SizeTumblingWindow<T>) window).getSize());
            case FIXED_TIME_TUMBLING_WINDOW:
                return new TimeFetchWindow(window.windowId(), ((FixedTimeTumblingWindow<T>) window).getTimeWindowSize());
            default:
                throw new GeaFlowDSLException("Not support window type:{}", window.getType());
        }
    }

}
