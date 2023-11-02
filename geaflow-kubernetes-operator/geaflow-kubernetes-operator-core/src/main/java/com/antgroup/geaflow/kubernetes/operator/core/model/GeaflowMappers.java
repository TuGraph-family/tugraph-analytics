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

package com.antgroup.geaflow.kubernetes.operator.core.model;

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import java.util.Collections;
import java.util.Set;

public class GeaflowMappers {

    private GeaflowMappers() {
    }

    public static SecondaryToPrimaryMapper<Service> fromServiceSpecSelector(String nameKey) {
        return resource -> {
            final var spec = resource.getSpec();
            if (spec == null) {
                return Collections.emptySet();
            } else {
                final var map = spec.getSelector();
                if (map == null) {
                    return Collections.emptySet();
                }
                var name = map.get(nameKey);
                if (name == null) {
                    return Collections.emptySet();
                }
                var namespace = resource.getMetadata().getNamespace();
                return Set.of(new ResourceID(name, namespace));
            }
        };
    }

}
