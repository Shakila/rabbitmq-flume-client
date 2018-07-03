/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.wso2.rabbitmq.client.util;

/**
 * Holds constants of this RabbitMQ client
 */
public class Constants {
    public class Consumer {
        public static final String COUNTER_ACK = "rabbitmq.ack";
        public static final String COUNTER_EXCEPTION = "rabbitmq.exception";
        public static final String COUNTER_REJECT = "rabbitmq.reject";

        //Factory properties
        public static final String HOST_KEY = "host";
        public static final String PORT_KEY = "port";
        public static final String SSL_KEY = "ssl";
        public static final String VHOST_KEY = "virtual-host";
        public static final String USER_KEY = "username";
        public static final String PASSWORD_KEY = "password";
        public static final String QUEUE_KEY = "queue";
        public static final String AUTOACK_KEY = "auto-ack";
        public static final String PREFETCH_COUNT_KEY = "prefetch-count";
        public static final String TIMEOUT_KEY = "timeout";
        public static final String THREAD_COUNT_KEY = "threads";
        public static final String REQUEUING = "requeuing";

        public static final String CONSUMER_TAG = "rabbitMQConsumer";

        //Header properties
        public static final String EXCHANGE_KEY = "exchange";
        public static final String ROUTING_KEY = "routing-key";
        public static final String APP_ID = "app-id";
        public static final String CONTENT_ENCODING = "content-encoding";
        public static final String CONTENT_TYPE = "content-type";
        public static final String CORRELATION_ID = "correlation-id";
        public static final String DELIVERY_MODE = "delivery-mode";
        public static final String EXPIRATION = "expiration";
        public static final String MESSAGE_ID = "message-id";
        public static final String PRIORITY = "priority";
        public static final String REPLY_TO = "replyTo";
        public static final String TIMESTAMP = "timestamp";
        public static final String TYPE = "type";
        public static final String USER_ID = "timestamp";
    }
}
