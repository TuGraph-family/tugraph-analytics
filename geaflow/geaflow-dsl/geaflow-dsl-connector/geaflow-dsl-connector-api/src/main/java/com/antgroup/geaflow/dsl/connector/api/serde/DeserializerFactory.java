package com.antgroup.geaflow.dsl.connector.api.serde;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.config.keys.ConnectorConfigKeys;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.JsonDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.RowTableDeserializer;
import com.antgroup.geaflow.dsl.connector.api.serde.impl.TextDeserializer;
import com.antgroup.geaflow.dsl.connector.api.util.ConnectorConstants;

public class DeserializerFactory {

    public static <IN> TableDeserializer<IN> loadDeserializer(Configuration conf) {
        String connectorFormat = conf.getString(ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT,
                (String) ConnectorConfigKeys.GEAFLOW_DSL_CONNECTOR_FORMAT.getDefaultValue());
        if (connectorFormat.equals(ConnectorConstants.CONNECTOR_FORMAT_JSON)) {
            return (TableDeserializer<IN>) new JsonDeserializer();
        } else {
            return (TableDeserializer<IN>) new TextDeserializer();
        }
    }

    public static <IN> TableDeserializer<IN> loadRowTableDeserializer() {
        return (TableDeserializer<IN>) new RowTableDeserializer();
    }

    public static <IN> TableDeserializer<IN> loadTextDeserializer() {
        return (TableDeserializer<IN>) new TextDeserializer();
    }

}
