/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geaflow.kubernetes.operator.core.job;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.geaflow.kubernetes.operator.core.model.customresource.GeaflowJob;
import org.apache.geaflow.kubernetes.operator.core.util.KubernetesUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class GeaflowJobApiService implements JobApiService<GeaflowJob> {

    @Autowired
    private JobCRCache jobCRCache;

    @Override
    public void createJob(GeaflowJob geaflowJob) {
        KubernetesUtil.createGeaflowJob(geaflowJob);
    }

    @Override
    public void deleteJob(String appId) {
        KubernetesUtil.deleteGeaflowJob(appId);
    }

    public Collection<GeaflowJob> queryJobs() {
        return jobCRCache.getGeaflowJobCRs();
    }
}
