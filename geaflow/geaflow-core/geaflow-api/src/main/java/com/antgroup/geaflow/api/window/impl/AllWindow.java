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

import com.antgroup.geaflow.api.window.IWindow;

public class AllWindow<T> implements IWindow<T> {

    private static final long DEFAULT_GLOBAL_WINDOW_ID = 0;

    private AllWindow() {
    }

    @Override
    public long windowId() {
        return DEFAULT_GLOBAL_WINDOW_ID;
    }

    @Override
    public void initWindow(long windowId) {
    }

    @Override
    public long assignWindow(T value) {
        return DEFAULT_GLOBAL_WINDOW_ID;
    }

    public static synchronized <T> AllWindow<T> getInstance() {
        return new AllWindow<>();
    }
}
