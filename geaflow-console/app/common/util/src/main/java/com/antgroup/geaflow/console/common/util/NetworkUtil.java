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

package com.antgroup.geaflow.console.common.util;

import com.antgroup.geaflow.console.common.util.exception.GeaflowException;
import com.antgroup.geaflow.console.common.util.exception.GeaflowIllegalException;
import com.google.common.base.Preconditions;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

public class NetworkUtil {

    public static final String LOCALHOST = "127.0.0.1";

    private static final Map<String, Integer> DEFAULT_PORT_MAP = new HashMap<>();

    private static String HOST_NAME;

    static {
        DEFAULT_PORT_MAP.put("http", 80);
        DEFAULT_PORT_MAP.put("https", 443);
        DEFAULT_PORT_MAP.put("hdfs", 9000);
        DEFAULT_PORT_MAP.put("dfs", 9000);
        DEFAULT_PORT_MAP.put("jdbc:mysql", 3306);
    }

    public static String getHostName() {
        if (HOST_NAME != null) {
            return HOST_NAME;
        }

        try {
            return HOST_NAME = InetAddress.getLocalHost().getHostName();

        } catch (Exception e) {
            throw new GeaflowException("Init local hostname failed", e);
        }
    }

    public static boolean isLocal(String url) {
        return LOCALHOST.equals(getIp(getHost(url)));
    }

    public static String getHost(String url) {
        if (StringUtils.isBlank(url)) {
            throw new GeaflowIllegalException("Invalid url");
        }

        if (url.contains("://")) {
            url = StringUtils.substringAfter(url, "://");
        }

        String[] seps = new String[]{":", "/", "?", "#"};
        for (String sep : seps) {
            if (url.contains(sep)) {
                url = StringUtils.substringBefore(url, sep);
            }
        }

        return url;
    }

    public static Integer getPort(String url) {
        if (StringUtils.isBlank(url)) {
            throw new GeaflowIllegalException("Invalid url");
        }

        Integer port = null;
        if (url.contains("://")) {
            String schema = StringUtils.substringBefore(url, "://");
            port = NetworkUtil.getDefaultPort(schema);

            url = StringUtils.substringAfter(url, "://");
        }

        String[] seps = new String[]{"/", "?", "#"};
        for (String sep : seps) {
            if (url.contains(sep)) {
                url = StringUtils.substringBefore(url, sep);
            }
        }

        if (url.contains(":")) {
            port = Integer.parseInt(StringUtils.substringAfter(url, ":"));
        }

        return port;
    }

    public static String getIp(String hostname) {
        try {
            return InetAddress.getByName(hostname).getHostAddress();

        } catch (Exception e) {
            throw new GeaflowIllegalException("Invalid hostname {}", hostname);
        }
    }

    public static Integer getDefaultPort(String schema) {
        return DEFAULT_PORT_MAP.get(schema);
    }

    public static void testUrls(String urls, String sep) {
        String[] list = StringUtils.splitByWholeSeparator(urls, sep);
        if (ArrayUtils.isEmpty(list)) {
            throw new GeaflowIllegalException("Invalid urls {}", urls);
        }

        for (String url : list) {
            NetworkUtil.testUrl(url);
        }
    }

    public static void testUrl(String url) {
        String host = NetworkUtil.getHost(url);
        Integer port = NetworkUtil.getPort(url);
        if (port == null) {
            throw new GeaflowIllegalException("Port is needed of url {}", url);
        }
        testHostPort(host, port);
    }

    public static void testHostPort(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
            Preconditions.checkArgument(socket.isConnected(), "Socket is not connected");

        } catch (Exception e) {
            throw new GeaflowIllegalException("Connect to {}:{} failed, {}", host, port, e.getMessage(), e);
        }
    }
}
