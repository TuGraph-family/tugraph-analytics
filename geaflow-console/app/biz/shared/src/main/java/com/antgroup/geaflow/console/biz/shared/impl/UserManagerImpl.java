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

import com.antgroup.geaflow.console.biz.shared.UserManager;
import com.antgroup.geaflow.console.biz.shared.convert.NameViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.UserViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.UserView;
import com.antgroup.geaflow.console.common.dal.entity.UserEntity;
import com.antgroup.geaflow.console.common.dal.model.UserSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.context.ContextHolder;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.antgroup.geaflow.console.core.model.security.GeaflowUser;
import com.antgroup.geaflow.console.core.service.NameService;
import com.antgroup.geaflow.console.core.service.UserService;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserManagerImpl extends NameManagerImpl<GeaflowUser, UserView, UserSearch> implements UserManager {

    @Autowired
    private UserService userService;

    @Autowired
    private UserViewConverter userViewConverter;

    @Override
    protected NameViewConverter<GeaflowUser, UserView> getConverter() {
        return userViewConverter;
    }

    @Override
    protected List<GeaflowUser> parse(List<UserView> views) {
        return ListUtil.convert(views, userViewConverter::convert);
    }

    @Override
    protected NameService<GeaflowUser, UserEntity, UserSearch> getService() {
        return userService;
    }

    @Transactional
    @Override
    public String register(UserView view) {
        GeaflowUser user = userViewConverter.convert(view);
        return userService.createUser(user);
    }

    @Override
    public Map<String, String> getUserNames(Collection<String> userIds) {
        return userService.getUserNames(userIds);
    }

    @Override
    public UserView getUser(String userId) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (!systemSession && !userService.existTenantUser(tenantId, userId)) {
            throw new GeaflowIllegalException("User not found");
        }

        return get(userId);
    }

    @Override
    public String addUser(UserView view) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (systemSession) {
            throw new GeaflowIllegalException("Use user register instead");
        }

        String userName = view.getName();
        String userId = userService.getIdByName(userName);
        userService.addTenantUser(tenantId, userId);
        return userId;
    }

    @Override
    public boolean updateUser(String userId, UserView view) {
        if (!ContextHolder.get().getUserId().equals(userId)) {
            throw new GeaflowIllegalException("Change other user failed");
        }

        return updateById(userId, view);
    }

    @Override
    public boolean deleteUser(String userId) {
        boolean systemSession = ContextHolder.get().isSystemSession();
        String tenantId = ContextHolder.get().getTenantId();
        if (systemSession) {
            throw new GeaflowIllegalException("Permanently deleted user not allowed");
        }

        return userService.deleteTenantUser(tenantId, userId);
    }
}
