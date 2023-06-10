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

package com.antgroup.geaflow.console.core.service;

import com.antgroup.geaflow.console.common.dal.dao.NameDao;
import com.antgroup.geaflow.console.common.dal.dao.TenantUserMappingDao;
import com.antgroup.geaflow.console.common.dal.dao.UserDao;
import com.antgroup.geaflow.console.common.dal.entity.UserEntity;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.UserSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.core.model.security.GeaflowRole;
import com.antgroup.geaflow.console.core.model.security.GeaflowUser;
import com.antgroup.geaflow.console.core.service.converter.NameConverter;
import com.antgroup.geaflow.console.core.service.converter.UserConverter;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService extends NameService<GeaflowUser, UserEntity, UserSearch> {

    @Autowired
    private UserDao userDao;

    @Autowired
    private UserConverter userConverter;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private TenantUserMappingDao tenantUserMappingDao;

    @Override
    protected NameDao<UserEntity, UserSearch> getDao() {
        return userDao;
    }

    @Override
    protected NameConverter<GeaflowUser, UserEntity> getConverter() {
        return userConverter;
    }

    @Override
    protected List<GeaflowUser> parse(List<UserEntity> entities) {
        return ListUtil.convert(entities, e -> userConverter.convert(e));
    }

    @Override
    public PageList<GeaflowUser> search(UserSearch search) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        if (systemSession) {
            return super.search(search);
        }

        String tenantId = ContextHolder.get().getTenantId();
        return userDao.search(tenantId, search).transform(this::parse);
    }

    @Transactional
    public String createUser(GeaflowUser user) {
        if (userDao.existName(user.getName())) {
            throw new IllegalArgumentException("User exists");
        }

        // create user
        String userId = create(user);

        // init user id context when register success
        ContextHolder.get().setUserId(userId);

        // init the first user as system admin
        if (userDao.count() == 1) {
            authorizationService.addRole(null, userId, GeaflowRole.SYSTEM_ADMIN);
        }

        return userId;
    }

    public Map<String, String> getUserNames(Collection<String> userIds) {
        return userDao.getUserNames(userIds);
    }

    public boolean existTenantUser(String tenantId, String userId) {
        return tenantUserMappingDao.existUser(tenantId, userId);
    }

    public boolean addTenantUser(String tenantId, String userId) {
        if (tenantUserMappingDao.existUser(tenantId, userId)) {
            throw new GeaflowIllegalException("Tenant user exists");
        }

        Preconditions.checkNotNull(tenantId, "Invalid tenantId");
        Preconditions.checkArgument(userDao.exist(userId), "User not exists");
        return tenantUserMappingDao.addUser(tenantId, userId);
    }

    public boolean deleteTenantUser(String tenantId, String userId) {
        return tenantUserMappingDao.deleteUser(tenantId, userId);
    }
}

