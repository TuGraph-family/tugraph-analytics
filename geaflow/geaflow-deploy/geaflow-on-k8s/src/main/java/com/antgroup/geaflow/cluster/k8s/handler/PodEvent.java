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
import io.fabric8.kubernetes.api.model.Pod;
import java.io.Serializable;

public class PodEvent implements Serializable {

    private EventKind eventKind;
    private String hostIp;
    private String podIp;
    private long ts;
    private String containerId;

    public PodEvent(Pod pod, EventKind kind) {
        this(pod, kind, System.currentTimeMillis());
    }

    public PodEvent(Pod pod, EventKind kind, long ts) {
        this.eventKind = kind;
        this.containerId = KubernetesUtils.extractComponentId(pod);
        this.podIp = pod.getStatus().getPodIP();
        this.hostIp = pod.getStatus().getHostIP();
        this.ts = ts;
    }

    public EventKind getEventKind() {
        return eventKind;
    }

    public void setEventKind(EventKind eventKind) {
        this.eventKind = eventKind;
    }

    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp;
    }

    public String getPodIp() {
        return podIp;
    }

    public void setPodIp(String podIp) {
        this.podIp = podIp;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    @Override
    public String toString() {
        return "PodEvent{" + "eventKind=" + eventKind + ", hostIp='" + hostIp + '\'' + ", podIp='"
            + podIp + '\'' + ", ts=" + ts + ", containerId='" + containerId + '\'' + '}';
    }
}
