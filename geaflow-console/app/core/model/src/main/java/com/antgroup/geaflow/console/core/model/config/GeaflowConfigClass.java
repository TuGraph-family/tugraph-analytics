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

package com.antgroup.geaflow.console.core.model.config;

import com.alibaba.fastjson.JSON;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import lombok.Getter;

@Getter
public abstract class GeaflowConfigClass {

    private final GeaflowConfig extendConfig = new GeaflowConfig();

    public final GeaflowConfig build() {
        GeaflowConfigDesc configDesc = ConfigDescFactory.getOrRegister(getClass());

        try {
            GeaflowConfig config = new GeaflowConfig();
            for (ConfigDescItem item : configDesc.getItems()) {
                String key = item.getKey();
                Object value = item.getField().get(this);
                if (value == null) {
                    continue;
                }

                if (GeaflowConfigType.CONFIG.equals(item.getType())) {
                    // build inner config
                    buildInner(config, item, (GeaflowConfigClass) value);

                } else {
                    config.put(key, value);
                }
            }

            // add extend config
            config.putAll(this.getExtendConfig());

            // validate config
            configDesc.validateConfig(config);

            return config;

        } catch (Exception e) {
            throw new GeaflowException("Build config of {} instance failed", getClass().getName(), e);
        }
    }

    private void buildInner(GeaflowConfig config, ConfigDescItem item, GeaflowConfigClass innerConfigClass) {
        String key = item.getKey();
        ConfigValueBehavior behavior = item.getBehavior();

        GeaflowConfig innerConfig = innerConfigClass.build();

        // process behavior
        switch (behavior) {
            case NESTED:
                config.put(key, innerConfig);
                break;
            case FLATTED:
                config.putAll(innerConfig);
                break;
            case JSON:
                GeaflowConfigDesc innerConfigDesc = ConfigDescFactory.getOrRegister(innerConfigClass.getClass());
                innerConfigDesc.getItems().forEach(innerItem -> {
                    if (innerItem.isJsonIgnore()) {
                        innerConfig.remove(innerItem.getKey());
                    }
                });
                config.put(key, JSON.toJSONString(innerConfig));
                break;
            default:
                throw new GeaflowException("Unsupported config value behavior {}", behavior);
        }
    }

}
