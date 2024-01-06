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

package com.antgroup.geaflow.kubernetes.operator.core.model.customresource;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ComponentResource {

    /**
     * Cpu cores.
     */
    private Double cpuCores;

    /**
     * Memory with unit Mb.
     */
    private Integer memoryMb;

    /**
     * Jvm args. Seperated with comma.
     * e.g. -Xmx1024m,-Xms1024m,-Xn512m
     */
    private String jvmOptions;
}
