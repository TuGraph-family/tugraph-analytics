/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.dao.ViewDao;
import com.antgroup.geaflow.console.common.dal.entity.ViewEntity;
import com.antgroup.geaflow.console.common.dal.model.ViewSearch;
import com.antgroup.geaflow.console.core.model.data.GeaflowView;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.ViewConverter;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ViewService extends NameService<GeaflowView, ViewEntity, ViewSearch> {

    @Autowired
    private ViewDao viewDao;

    @Autowired
    private ViewConverter viewConverter;

    @Override
    protected NameDao<ViewEntity, ViewSearch> getDao() {
        return viewDao;
    }

    @Override
    protected NameConverter<GeaflowView, ViewEntity> getConverter() {
        return viewConverter;
    }

    @Override
    protected List<GeaflowView> parse(List<ViewEntity> viewEntities) {
        return null;
    }

}

