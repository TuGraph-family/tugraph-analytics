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

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class UserSpec {

    /**
     * The unique id of the job.
     * Optional. it will be replaced to a random number if not filled.
     */
    Long jobUid;

    /**
     * The metric config of the job.
     */
    Map<String, String> metricConfig;

    /**
     * The state config of the job.
     */
    Map<String, String> stateConfig;

    /**
     * Other additional args.
     */
    Map<String, String> additionalArgs;

}
