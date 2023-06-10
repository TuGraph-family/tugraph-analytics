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

package com.antgroup.geaflow.cluster.resourcemanager;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;

import com.antgroup.geaflow.cluster.clustermanager.AbstractClusterManager;
import com.antgroup.geaflow.cluster.clustermanager.IClusterManager;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesResourceManager extends DefaultResourceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesResourceManager.class);

    private static final String LABEL_CONTAINER_INDEX_PATTERN = "container-index-%s";

    private static final int MASTER_COMPONENT_ID = 0;

    public KubernetesResourceManager(IClusterManager clusterManager) {
        super(clusterManager);
    }

    @Override
    public void init(ResourceManagerContext context) {
        String clusterId = context.getConfig().getString(CLUSTER_ID);
        String containerIndexLabel = String.format(LABEL_CONTAINER_INDEX_PATTERN, clusterId);
        Set<Integer> containerIndex = super.metaKeeper.getComponentIds(containerIndexLabel);
        LOGGER.info("get container index {} of label {} from backend", containerIndex, containerIndexLabel);

        boolean isRecover = containerIndex != null;
        context.setRecover(isRecover);
        super.init(context);

        if (isRecover) {
            LOGGER.info("recover container index {} of label {} from backend", containerIndex, containerIndexLabel);
            ((AbstractClusterManager) this.clusterManager).setContainerIds(containerIndex);
            ((AbstractClusterManager) this.clusterManager).clusterFailover(MASTER_COMPONENT_ID);
        } else {
            containerIndex = ((AbstractClusterManager) this.clusterManager).getContainerIds();
            this.metaKeeper.saveComponentIndex(containerIndexLabel, containerIndex);
            LOGGER.info("saved container index {} of label {} to backend", containerIndex, containerIndexLabel);
        }
    }
}
