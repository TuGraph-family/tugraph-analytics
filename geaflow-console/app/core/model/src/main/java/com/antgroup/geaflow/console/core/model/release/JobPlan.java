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

package com.antgroup.geaflow.console.core.model.release;

import com.alibaba.fastjson.JSON;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobPlan {

    private final Map<String, PlanVertex> vertices = new LinkedHashMap<>();

    private final Map<String, List<PlanEdge>> edgeMap = new LinkedHashMap<>();

    public static JobPlan build(String json) {
        return JSON.parseObject(json, JobPlan.class);
    }

    public String toJsonString() {
        return JSON.toJSONString(this);
    }

    @Getter
    @Setter
    public static class PlanVertex {

        private String key;

        private int parallelism;

        private JobPlan innerPlan;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class PlanEdge {

        private String sourceKey;

        private String targetKey;

        private String type;
    }


}
