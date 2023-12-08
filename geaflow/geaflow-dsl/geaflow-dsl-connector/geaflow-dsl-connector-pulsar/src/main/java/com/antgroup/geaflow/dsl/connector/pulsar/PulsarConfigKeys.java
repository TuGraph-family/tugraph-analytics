package com.antgroup.geaflow.dsl.connector.pulsar;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class PulsarConfigKeys {

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SERVERS = ConfigKeys
            .key("geaflow.dsl.pulsar.servers")
            .noDefaultValue()
            .description("The pulsar bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_PORT = ConfigKeys
            .key("geaflow.dsl.pulsar.port")
            .noDefaultValue()
            .description("The pulsar bootstrap servers list.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_TOPIC = ConfigKeys
            .key("geaflow.dsl.pulsar.topic")
            .noDefaultValue()
            .description("The pulsar topic.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SUBSCRIBE_NAME = ConfigKeys
            .key("geaflow.dsl.pulsar.subscribeName")
            .defaultValue("default-subscribeName")
            .description("The pulsar subscribeName, default is 'default-subscribeName'.");

    public static final ConfigKey GEAFLOW_DSL_PULSAR_SUBSCRIBE_INITIAL_POSITION = ConfigKeys
            .key("geaflow.dsl.pulsar.subscriptionInitialPosition")
            .defaultValue("latest")
            .description("The pulsar subscriptionInitialPosition, default is 'default-subscriptionInitialPosition'.");


}
