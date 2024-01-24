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

package com.antgroup.geaflow.cluster.k8s.handler;

import com.antgroup.geaflow.cluster.k8s.handler.PodHandlerRegistry.EventKind;
import com.antgroup.geaflow.cluster.k8s.utils.KubernetesUtils;
import com.antgroup.geaflow.stats.model.ExceptionLevel;
import io.fabric8.kubernetes.api.model.Pod;

public class PodDeletedHandler extends AbstractPodHandler {

    @Override
    public void handle(Pod pod) {
        String componentId = KubernetesUtils.extractComponentId(pod);
        if (componentId != null) {
            String deleteMessage = String.format("Pod #%s %s is deleted.",
                componentId, pod.getMetadata().getName());
            PodEvent event = new PodEvent(pod, EventKind.POD_DELETED);
            reportPodEvent(event, ExceptionLevel.ERROR, deleteMessage);
        }
    }
}
