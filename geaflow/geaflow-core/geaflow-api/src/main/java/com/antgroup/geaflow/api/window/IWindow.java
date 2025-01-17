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

package com.antgroup.geaflow.api.window;

import com.antgroup.geaflow.api.function.Function;
import java.io.Serializable;

public interface IWindow<T> extends Function, Serializable {

    /**
     * Returns the window id.
     */
    long windowId();

    /**
     * Initialize window with windowId.
     */
    void initWindow(long windowId);

    /**
     * Assign window id for value.
     */
    long assignWindow(T value);

    /**
     * Return window type.
     */
    WindowType getType();

}
