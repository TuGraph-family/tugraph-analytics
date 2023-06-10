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

import com.antgroup.geaflow.console.biz.shared.view.IdView;
import com.antgroup.geaflow.console.common.dal.model.IdSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import java.util.List;

public interface IdManager<V extends IdView, S extends IdSearch> {

    PageList<V> search(S search);

    V get(String id);

    String create(V view);

    boolean updateById(String id, V view);

    boolean drop(String id);

    List<V> get(List<String> ids);

    List<String> create(List<V> views);

    boolean update(List<V> views);

    boolean drop(List<String> ids);
}
