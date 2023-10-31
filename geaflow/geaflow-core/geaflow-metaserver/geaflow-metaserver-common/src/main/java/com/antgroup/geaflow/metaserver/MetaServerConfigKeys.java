package com.antgroup.geaflow.metaserver;

import com.antgroup.geaflow.common.config.ConfigKey;
import com.antgroup.geaflow.common.config.ConfigKeys;

public class MetaServerConfigKeys {

    public static final ConfigKey META_SERVER_SERVICE_TYPE = ConfigKeys
        .key("geaflow.meta.server.service.type")
        .defaultValue("local")
        .description("meta server service type eg[LOCAL,STATE_SERVICE]");

}
