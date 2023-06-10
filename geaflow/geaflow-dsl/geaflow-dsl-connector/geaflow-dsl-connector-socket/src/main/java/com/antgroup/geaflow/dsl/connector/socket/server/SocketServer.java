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

package com.antgroup.geaflow.dsl.connector.socket.server;

public class SocketServer {

    private static final String TERMINAL_SERVER_TYPE = "TERMINAL";

    private static final String GRAPH_INSIGHT_SERVER_TYPE = "GI";

    public static void main(String[] args) {
        int port = 9003;
        String serverType = TERMINAL_SERVER_TYPE;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            serverType = String.valueOf(args[1]);
        }
        if (serverType.equalsIgnoreCase(GRAPH_INSIGHT_SERVER_TYPE)) {
            new NettyWebServer().bind(port);
        } else {
            new NettyTerminalServer().bind(port);
        }
    }
}
