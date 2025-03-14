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

import com.antgroup.geaflow.console.common.dal.dao.AuditDao;
import com.antgroup.geaflow.console.common.dal.dao.IdDao;
import com.antgroup.geaflow.console.common.dal.entity.AuditEntity;
import com.antgroup.geaflow.console.common.dal.model.AuditSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.core.model.runtime.GeaflowAudit;
import com.antgroup.geaflow.console.core.service.converter.AuditConverter;
import com.antgroup.geaflow.console.core.service.converter.IdConverter;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuditService extends IdService<GeaflowAudit, AuditEntity, AuditSearch> {

    @Autowired
    private AuditDao auditDao;

    @Autowired
    private AuditConverter auditConverter;

    @Override
    protected IdDao<AuditEntity, AuditSearch> getDao() {
        return auditDao;
    }

    @Override
    protected IdConverter<GeaflowAudit, AuditEntity> getConverter() {
        return auditConverter;
    }

    @Override
    protected List<GeaflowAudit> parse(List<AuditEntity> auditEntities) {
        return ListUtil.convert(auditEntities, e -> auditConverter.convert(e));
    }
}

