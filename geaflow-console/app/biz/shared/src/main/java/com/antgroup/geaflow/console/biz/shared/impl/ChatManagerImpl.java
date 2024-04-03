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

import com.antgroup.geaflow.console.biz.shared.ChatManager;
import com.antgroup.geaflow.console.biz.shared.convert.ChatViewConverter;
import com.antgroup.geaflow.console.biz.shared.convert.IdViewConverter;
import com.antgroup.geaflow.console.biz.shared.view.ChatView;
import com.antgroup.geaflow.console.common.dal.model.ChatSearch;
import com.antgroup.geaflow.console.common.util.ListUtil;
import com.antgroup.geaflow.console.common.util.type.GeaflowStatementStatus;
import com.antgroup.geaflow.console.core.model.llm.GeaflowChat;
import com.antgroup.geaflow.console.core.service.ChatService;
import com.antgroup.geaflow.console.core.service.IdService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ChatManagerImpl extends IdManagerImpl<GeaflowChat, ChatView, ChatSearch> implements ChatManager {

    @Autowired
    private ChatService chatService;

    @Autowired
    private ChatViewConverter chatViewConverter;

    @Override
    protected IdService<GeaflowChat, ?, ChatSearch> getService() {
        return chatService;
    }

    @Override
    protected IdViewConverter<GeaflowChat, ChatView> getConverter() {
        return chatViewConverter;
    }

    
    @Override
    protected List<GeaflowChat> parse(List<ChatView> views) {
        return ListUtil.convert(views, chatViewConverter::convert);
    }
    
    @Override
    public String callASync(ChatView view, boolean withSchema) {
        view.setStatus(GeaflowStatementStatus.RUNNING);
        String id = super.create(view);
        GeaflowChat chat = chatViewConverter.convert(view);
        chatService.callASync(chat, withSchema);

        return id;
    }
    
    @Override
    public String callSync(ChatView chatView, boolean saveRecord, boolean withSchema) {
        GeaflowChat geaflowChat = chatViewConverter.convert(chatView);
        return chatService.callSync(geaflowChat, saveRecord, withSchema);
    }

    @Override
    public boolean dropByJobId(String jobId) {
        return chatService.dropByJobId(jobId);
    }


}
