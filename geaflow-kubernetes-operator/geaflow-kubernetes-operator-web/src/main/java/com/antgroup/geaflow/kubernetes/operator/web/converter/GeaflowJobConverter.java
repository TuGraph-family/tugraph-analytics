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

package com.antgroup.geaflow.kubernetes.operator.web.converter;

import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.GeaflowJobStatus;
import com.antgroup.geaflow.kubernetes.operator.web.view.GeaflowJobView;

public class GeaflowJobConverter {

    public static GeaflowJob convert2GeaflowJob(GeaflowJobView view) {
        GeaflowJob job = new GeaflowJob();
        job.getMetadata().setName(view.getName());
        job.setSpec(view.getSpec());
        job.setStatus(new GeaflowJobStatus());
        return job;
    }

}
