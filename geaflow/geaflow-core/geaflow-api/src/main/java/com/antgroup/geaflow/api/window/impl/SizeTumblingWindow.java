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

package com.antgroup.geaflow.api.window.impl;

import com.antgroup.geaflow.api.window.ITumblingWindow;

public class SizeTumblingWindow<T> implements ITumblingWindow<T> {

    private long size;
    private long count;
    private long windowId;

    public SizeTumblingWindow(long size) {
        this.size = size;
        this.count = 0;
    }

    public long getSize() {
        return size;
    }

    public static <T> SizeTumblingWindow<T> of(long size) {
        return new SizeTumblingWindow(size);
    }

    @Override
    public long windowId() {
        return this.windowId;
    }

    @Override
    public void initWindow(long windowId) {
        this.windowId = windowId;
        this.count = 0;
    }

    @Override
    public long assignWindow(T value) {
        if (count++ < size) {
            return windowId;
        } else {
            return windowId + 1;
        }
    }
}
