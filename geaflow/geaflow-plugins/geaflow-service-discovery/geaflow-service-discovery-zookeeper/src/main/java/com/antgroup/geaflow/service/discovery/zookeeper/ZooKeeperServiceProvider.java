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
import com.antgroup.geaflow.service.discovery.ServiceListener;
import com.antgroup.geaflow.service.discovery.ServiceProvider;

public class ZooKeeperServiceProvider implements ServiceProvider {

    private final ZooKeeperWatcher watcher;

    public ZooKeeperServiceProvider(Configuration configuration) {
        watcher = new ZooKeeperWatcher(configuration, true);
    }

    @Override
    public boolean exists(String path) {
        return ZKUtil.exists(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public boolean watchAndCheckExists(String path) {
        return ZKUtil.watchAndCheckExists(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public void delete(String path) {
        ZKUtil.deleteNode(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public byte[] getDataAndWatch(String path) {
        return ZKUtil.getDataAndWatch(watcher, ZKUtil.joinZNode(watcher.baseZNode, path));
    }

    @Override
    public boolean createAndWatch(String path, byte[] data) {
        return ZKUtil.createEphemeralNodeAndWatch(watcher,
            ZKUtil.joinZNode(watcher.baseZNode, path), data);
    }

    @Override
    public boolean update(String path, byte[] data) {
        return ZKUtil.updatePersistentNode(watcher, ZKUtil.joinZNode(watcher.baseZNode, path),
            data);
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
