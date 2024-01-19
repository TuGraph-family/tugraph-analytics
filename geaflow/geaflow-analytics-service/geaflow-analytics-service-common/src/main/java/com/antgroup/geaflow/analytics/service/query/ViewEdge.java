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

package com.antgroup.geaflow.analytics.service.query;

import java.util.Map;

public class ViewEdge {

    private final String source;
    private final String target;
    private final String label;
    private final String direction;
    private final Map<String, Object> properties;

    public ViewEdge(String source, String target, String label, Map<String, Object> properties, String direction) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.properties = properties;
        this.direction = direction;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getLabel() {
        return label;
    }


    public String getDirection() {
        return direction;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

}
