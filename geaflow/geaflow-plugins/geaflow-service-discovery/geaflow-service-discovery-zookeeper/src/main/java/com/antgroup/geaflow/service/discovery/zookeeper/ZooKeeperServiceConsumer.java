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

package com.antgroup.geaflow.service.discovery.zookeeper;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.service.discovery.ServiceConsumer;
import com.antgroup.geaflow.service.discovery.ServiceListener;

public class ZooKeeperServiceConsumer implements ServiceConsumer {

    private final ZooKeeperWatcher watcher;

    public ZooKeeperServiceConsumer(Configuration configuration) {
        watcher = new ZooKeeperWatcher(configuration);
    }

    @Override
    public boolean exists(String path) {
        return ZKUtil.exists(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public byte[] getDataAndWatch(String path) {
        return ZKUtil.getDataAndWatch(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public void register(ServiceListener listener) {
        watcher.registerListener(listener);
    }

    @Override
    public void close() {
        watcher.close();
    }


}
