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

package com.antgroup.geaflow.console.biz.shared.impl;

import com.antgroup.geaflow.console.biz.shared.InstanceManager;
import com.antgroup.geaflow.console.biz.shared.convert.InstanceViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.InstanceView;
import com.antgroup.geaflow.console.common.dal.model.InstanceSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.data.GeaflowInstance;
import com.antgroup.geaflow.console.core.service.InstanceService;
import com.antgroup.geaflow.console.core.service.NameService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class InstanceManagerImpl extends NameManagerImpl<GeaflowInstance, InstanceView, InstanceSearch> implements
    InstanceManager {

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private InstanceViewConverter instanceViewConverter;

    @Override
    public List<InstanceView> search() {
        List<GeaflowInstance> models = instanceService.search();
        return ListUtil.convert(models, e -> instanceViewConverter.convert(e));
    }

    @Override
    protected NameViewConverter<GeaflowInstance, InstanceView> getConverter() {
        return instanceViewConverter;
    }

    @Override
    protected List<GeaflowInstance> parse(List<InstanceView> views) {
        return ListUtil.convert(views, e -> instanceViewConverter.convert(e));
    }

    @Override
    protected NameService<GeaflowInstance, ?, InstanceSearch> getService() {
        return instanceService;
    }
}
