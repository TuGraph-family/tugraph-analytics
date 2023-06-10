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

package com.antgroup.geaflow.console.common.dal.datasource;

import com.antgroup.geaflow.console.common.dal.dao.IdDao;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.context.GeaflowContext;
import com.baomidou.mybatisplus.extension.plugins.handler.TenantLineHandler;
import com.google.common.base.Preconditions;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;

public class TenantInterceptorHandler implements TenantLineHandler {

    @Override
    public Expression getTenantId() {
        GeaflowContext context = ContextHolder.get();
        Preconditions.checkNotNull(context, "Invalid context");

        // allow null tenant id
        String tenantId = context.getTenantId();
        if (tenantId == null) {
            return null;
        }

        return new StringValue(tenantId);
    }

    @Override
    public String getTenantIdColumn() {
        return IdDao.TENANT_ID_FIELD_NAME;
    }

    @Override
    public boolean ignoreTable(String tableName) {
        // allow null tenant id
        return ContextHolder.get().getTenantId() == null;
    }
}
