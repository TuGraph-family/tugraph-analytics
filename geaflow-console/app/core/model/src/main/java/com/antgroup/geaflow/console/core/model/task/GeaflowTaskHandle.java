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

package com.antgroup.geaflow.console.core.model.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginType;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Setter
@Getter
public abstract class GeaflowTaskHandle {

    // cluster type
    protected GeaflowPluginType clusterType;

    // app id
    protected String appId;

    // startup notify
    protected StartupNotifyInfo startupNotifyInfo;


    public static GeaflowTaskHandle parse(String text) {
        if (StringUtils.isEmpty(text)) {
            return null;
        }

        JSONObject json = JSON.parseObject(text);
        GeaflowPluginType clusterType = GeaflowPluginType.valueOf(json.get("clusterType").toString());
        switch (clusterType) {
            case CONTAINER:
                return json.toJavaObject(ContainerTaskHandle.class);
            case K8S:
                return json.toJavaObject(K8sTaskHandle.class);
            case RAY:
                return json.toJavaObject(RayTaskHandle.class);
            default:
                throw new GeaflowException("Unsupported cluster type {}", clusterType);
        }
    }

    @Setter
    @Getter
    public static class StartupNotifyInfo {

        private String masterAddress;

        private String driverAddress;

        private String clientAddress;

    }

}
