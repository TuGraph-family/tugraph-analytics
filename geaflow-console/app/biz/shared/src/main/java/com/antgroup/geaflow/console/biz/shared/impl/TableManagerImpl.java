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

import com.antgroup.geaflow.console.biz.shared.PluginConfigManager;
import com.antgroup.geaflow.console.biz.shared.TableManager;
import com.antgroup.geaflow.console.biz.shared.convert.DataViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.FieldViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.PluginConfigViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.TableViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.PluginConfigView;
import com.antgroup.geaflow.console.biz.shared.view.TableView;
import com.antgroup.geaflow.console.common.dal.entity.TableEntity;
import com.antgroup.geaflow.console.common.dal.model.TableSearch;
import com.antgroup.geaflow.console.common.util.Fmt;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowPluginCategory;
import com.antgroup.geaflow.console.core.model.data.GeaflowField;
import com.antgroup.geaflow.console.core.model.data.GeaflowTable;
import com.antgroup.geaflow.console.core.model.plugin.config.GeaflowPluginConfig;
import com.antgroup.geaflow.console.core.service.DataService;
import com.antgroup.geaflow.console.core.service.TableService;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TableManagerImpl extends DataManagerImpl<GeaflowTable, TableView, TableSearch> implements TableManager {

    @Autowired
    private TableService tableService;

    @Autowired
    private FieldViewConverter fieldViewConverter;

    @Autowired
    private PluginConfigViewConverter pluginConfigViewConverter;

    @Autowired
    private PluginConfigManager pluginConfigManager;

    @Autowired
    private TableViewConverter tableViewConverter;

    @Override
    public DataViewConverter<GeaflowTable, TableView> getConverter() {
        return tableViewConverter;
    }

    @Override
    public DataService<GeaflowTable, TableEntity, TableSearch> getService() {
        return tableService;
    }

    @Override
    public List<GeaflowTable> parse(List<TableView> views) {
        return views.stream().map(v -> {
            List<GeaflowField> fields = ListUtil.convert(v.getFields(), e -> fieldViewConverter.convert(e));
            GeaflowPluginConfig pluginConfig = pluginConfigViewConverter.convert(v.getPluginConfig());
            return tableViewConverter.convert(v, fields, pluginConfig);
        }).collect(Collectors.toList());
    }

    @Override
    public List<String> create(String instanceName, List<TableView> views) {
        for (TableView view : views) {
            PluginConfigView pluginConfigView = Preconditions.checkNotNull(view.getPluginConfig(),
                "Table pluginConfig is required");
            pluginConfigView.setCategory(GeaflowPluginCategory.TABLE);
            pluginConfigView.setName(Fmt.as("{}-{}-table-config", instanceName, view.getName()));
            pluginConfigManager.create(pluginConfigView);
        }
        return super.create(instanceName, views);
    }
}
