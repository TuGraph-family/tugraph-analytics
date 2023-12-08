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

package com.antgroup.geaflow.dsl.connector.pulsar.utils;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConstants {

    public static final SubscriptionType PULSAR_SUBSCRIBE_TYPE = SubscriptionType.Exclusive;
    public static final int PULSAR_NEGATIVE_ACK_REDELIVERY = 10;
    public static final TimeUnit PULSAR_NEGATIVE_ACK_REDELIVERY_UNIT = TimeUnit.SECONDS;
    public static final int PULSAR_MAX_REDELIVER_COUNT = 10;
    public static final int PULSAR_RECEIVER_QUEUE_SIZE = 1000;
    public static final int PULSAR_MAX_PENDING_MESSAGES = 1000;
    public static final int PULSAR_BATCHING_MAX_MESSAGES = 1000;
    public static final int PULSAR_BATCHING_MAX_PUBLISH_DELAY = 10;


}
