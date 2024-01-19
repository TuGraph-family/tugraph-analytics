package com.antgroup.geaflow.dsl.connector.pulsar;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.dsl.connector.api.TableReadableConnector;
import com.antgroup.geaflow.dsl.connector.api.TableSink;
import com.antgroup.geaflow.dsl.connector.api.TableSource;
import com.antgroup.geaflow.dsl.connector.api.TableWritableConnector;

public class PulsarTableConnector implements TableReadableConnector, TableWritableConnector {

    public static final String TYPE = "PULSAR";

    @Override
    public TableSource createSource(Configuration conf) {
        return new PulsarTableSource();
    }

    @Override
    public TableSink createSink(Configuration conf) {
        return new PulsarTableSink();
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
