package com.antgroup.geaflow.console.core.model.job.config;

import com.antgroup.geaflow.console.core.model.config.GeaflowConfigClass;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CompileContextClass extends GeaflowConfigClass {

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.token.key", comment = "token")
    @GeaflowConfigValue(required = true)
    private String tokenKey;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.instance.name", comment = "实例名")
    @GeaflowConfigValue(required = true)
    private String instanceName;

    @GeaflowConfigKey(value = "geaflow.dsl.catalog.type", comment = "作业Catalog存储类型")
    @GeaflowConfigValue(required = true, defaultValue = "memory")
    private String catalogType;

    @GeaflowConfigKey(value = "geaflow.gw.endpoint", comment = "console地址")
    @GeaflowConfigValue(required = true)
    private String endpoint;


}
