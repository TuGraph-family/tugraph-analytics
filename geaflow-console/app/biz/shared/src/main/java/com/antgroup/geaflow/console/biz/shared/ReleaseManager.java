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

package com.antgroup.geaflow.console.biz.shared;

import com.antgroup.geaflow.console.biz.shared.view.ReleaseUpdateView;
import com.antgroup.geaflow.console.biz.shared.view.ReleaseView;
import com.antgroup.geaflow.console.common.dal.model.ReleaseSearch;

public interface ReleaseManager extends IdManager<ReleaseView, ReleaseSearch> {

    String publish(String jobId);

    boolean updateRelease(String jobId, ReleaseUpdateView packageUpdate);
}
