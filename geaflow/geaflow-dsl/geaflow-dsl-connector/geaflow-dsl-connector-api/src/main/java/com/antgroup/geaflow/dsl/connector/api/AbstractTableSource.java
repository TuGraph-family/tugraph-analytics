package com.antgroup.geaflow.dsl.connector.api;


import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.connector.api.window.AllFetchWindow;
import com.antgroup.geaflow.dsl.connector.api.window.FetchWindow;
import com.antgroup.geaflow.dsl.connector.api.window.SizeFetchWindow;
import com.antgroup.geaflow.dsl.connector.api.window.TimeFetchWindow;
import java.io.IOException;
import java.util.Optional;

public abstract class AbstractTableSource implements TableSource {

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, FetchWindow windowInfo) throws IOException {
        switch (windowInfo.getType()) {
            case ALL_WINDOW:
                return fetch(partition, startOffset, (AllFetchWindow) windowInfo);
            case SIZE_TUMBLING_WINDOW:
                return fetch(partition, startOffset, (SizeFetchWindow) windowInfo);
            case FIXED_TIME_TUMBLING_WINDOW:
                return fetch(partition, startOffset, (TimeFetchWindow) windowInfo);
            default:
                throw new GeaFlowDSLException("Not support window type:{}", windowInfo.getType());
        }
    }


    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, AllFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, SizeFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

    public <T> FetchData<T> fetch(Partition partition, Optional<Offset> startOffset, TimeFetchWindow windowInfo) throws IOException {
        throw new GeaFlowDSLException("Not support");
    }

}
