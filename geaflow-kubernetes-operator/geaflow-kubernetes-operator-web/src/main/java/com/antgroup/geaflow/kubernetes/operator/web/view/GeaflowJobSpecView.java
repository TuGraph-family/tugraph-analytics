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

package com.antgroup.geaflow.kubernetes.operator.web.view;

import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.ClientSpec;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.ContainerSpec;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.DriverSpec;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.MasterSpec;
import com.antgroup.geaflow.kubernetes.operator.core.model.customresource.UserSpec;
import com.antgroup.geaflow.kubernetes.operator.core.model.job.RemoteFile;
import java.util.List;
import lombok.Data;

@Data
public class GeaflowJobSpecView {

    /**
     * Docker image to start the GeaFlow job.
     */
    protected String image;

    /**
     * Entry class of the job.
     */
    protected String entryClass;

    /**
     * Image pull policy of the docker image.
     * Optional. Owns a default value.
     */
    private String imagePullPolicy;

    /**
     * Kubernetes service account.
     * Optional. Owns a default value.
     */
    private String serviceAccount;

    /**
     * Engine jar files.
     * This is not needed when the image already contains an engine jar.
     */
    protected List<RemoteFile> engineJars;

    /**
     * User jar files.
     */
    protected List<RemoteFile> udfJars;

    /**
     * Spec for the client pod.
     */
    protected ClientSpec clientSpec;

    /**
     * Spec of the master.
     */
    protected MasterSpec masterSpec;

    /**
     * Spec of the drivers.
     */
    protected DriverSpec driverSpec;

    /**
     * Spec of the containers.
     */
    protected ContainerSpec containerSpec;

    /**
     * Spec of other user defined args.
     */
    protected UserSpec userSpec;

}
