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

package com.antgroup.geaflow.console.test;

import com.antgroup.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.PluginConfigView;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import com.antgroup.geaflow.console.core.model.config.ConfigDescFactory;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfig;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigKey;
import com.antgroup.geaflow.console.core.model.config.GeaflowConfigValue;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.model.plugin.config.PluginConfigClass;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PluginConfigViewConverterTest {

    PluginConfigViewConverter pluginConfigViewConverter = new PluginConfigViewConverter();

    @BeforeTest
    public void setUp() throws Exception {
        ConfigDescFactory.getOrRegister(TestPluginConfigView.class);
    }

    @Test
    public void test() {
        GeaflowConfig geaflowConfig = new GeaflowConfig();
        geaflowConfig.put("key.boolean", null);
        geaflowConfig.put("key.long", 1234L);
        geaflowConfig.put("key.double", 3.14);
        geaflowConfig.put("key.string", "hello");
        geaflowConfig.put("a", 1);
        geaflowConfig.put("b", "xxx");

        PluginConfigView view = new PluginConfigView();
        view.setId("xxxx");
        view.setName("test-plugin");
        view.setComment("test plugin");
        view.setType(GeaflowPluginType.MEMORY.name());
        view.setConfig(geaflowConfig);

        GeaflowPluginConfig model = pluginConfigViewConverter.convert(view);

        model.setGmtCreate(new Date());
        model.setGmtModified(new Date());
        PluginConfigView newView = pluginConfigViewConverter.convert(model);

        Assert.assertEquals(view.getConfig().size(), 6);
        Assert.assertEquals(newView.getConfig().size(), 6);
    }

    @Getter
    @Setter
    public static class TestPluginConfigView extends PluginConfigClass {

        @GeaflowConfigKey(value = "key.boolean")
        private Boolean booleanField;

        @GeaflowConfigKey(value = "key.long")
        @GeaflowConfigValue(required = true)
        private Long longField;

        @GeaflowConfigKey("key.double")
        @GeaflowConfigValue(defaultValue = "3.14")
        private Double doubleField;

        @GeaflowConfigKey(value = "key.string", comment = "String Value")
        @GeaflowConfigValue(required = true, defaultValue = "stringValue", masked = true)
        private String stringField;

        public TestPluginConfigView() {
            super(GeaflowPluginType.MEMORY);
        }
    }
}
