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

package com.antgroup.geaflow.api.pdata.stream.view;

import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.view.IViewDesc;
import com.antgroup.geaflow.view.PView;

public interface PStreamView<T> extends PView {

    /**
     * Initialize stream view.
     */
    PStreamView<T> init(IViewDesc viewDesc);

    /**
     * Append windowStream into incremental view.
     */
    PIncStreamView<T> append(PWindowStream<T> windowStream);

}
