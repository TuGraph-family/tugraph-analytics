/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.console.core.service.config;

import com.antgroup.geaflow.console.core.model.plugin.config.JdbcPluginConfigClass;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("spring.datasource")
@Getter
@Setter
public class DatasourceConfig {

    private String driverClassName;

    private String url;

    private String username;

    private String password;

    public JdbcPluginConfigClass buildPluginConfigClass() {
        JdbcPluginConfigClass jdbcConfig = new JdbcPluginConfigClass();
        jdbcConfig.setDriverClass(driverClassName);
        jdbcConfig.setUrl(url);
        jdbcConfig.setUsername(username);
        jdbcConfig.setPassword(password);
        return jdbcConfig;
    }

}
