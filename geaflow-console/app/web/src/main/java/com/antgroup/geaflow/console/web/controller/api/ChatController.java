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

package com.antgroup.geaflow.console.web.controller.api;

import static com.antgroup.geaflow.console.common.dal.dao.IdDao.CREATE_TIME_FIELD_NAME;

import com.antgroup.geaflow.console.biz.shared.ChatManager;
import com.antgroup.geaflow.console.biz.shared.view.ChatView;
import com.antgroup.geaflow.console.common.dal.model.ChatSearch;
import com.antgroup.geaflow.console.common.dal.model.PageList;
import com.antgroup.geaflow.console.common.dal.model.PageSearch.SortOrder;
import com.antgroup.geaflow.console.web.api.GeaflowApiResponse;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/chats")
public class ChatController {

    @Autowired
    private ChatManager chatManager;


    @GetMapping
    public GeaflowApiResponse<PageList<ChatView>> searchLLMs(ChatSearch search) {
        if (StringUtils.isEmpty(search.getSort())) {
            search.setOrder(SortOrder.ASC, CREATE_TIME_FIELD_NAME);
        }
        return GeaflowApiResponse.success(chatManager.search(search));
    }

    @PostMapping
    public GeaflowApiResponse<String> createChat(ChatView chatView,
                                                 @RequestParam(required = false) Boolean withSchema) {
        return GeaflowApiResponse.success(chatManager.callASync(chatView, withSchema));
    }

    @PostMapping("/callSync")
    public GeaflowApiResponse<String> callSync(ChatView chatView, 
                                               @RequestParam(required = false) Boolean saveRecord,
                                               @RequestParam(required = false) Boolean withSchema) {
        return GeaflowApiResponse.success(chatManager.callSync(chatView, saveRecord, withSchema));
    }

    @DeleteMapping("/jobs/{jobId}")
    public GeaflowApiResponse<Boolean> deleteChat(@PathVariable String jobId) {
        return GeaflowApiResponse.success(chatManager.dropByJobId(jobId));
    }

    
    
}
