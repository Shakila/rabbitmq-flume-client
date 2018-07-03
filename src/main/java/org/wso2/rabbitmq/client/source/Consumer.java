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
package org.wso2.rabbitmq.client.source;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.rabbitmq.client.util.Constants;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class Consumer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private Connection connection;
    private Channel channel;
    private ChannelProcessor channelProcessor;
    private CounterGroup counterGroup;
    private SourceCounter sourceCounter;

    private String hostname;
    private int port;
    private String username;
    private String password;
    private String virtualHost;
    private String queue;
    private boolean sslEnabled = false;
    private boolean autoAck = false;
    private boolean requeuing = false;
    private int prefetchCount = 0;
    private int timeout = -1;

    public Consumer() {
    }

    public Consumer setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public Consumer setPort(int port) {
        this.port = port;
        return this;
    }

    public Consumer setSSLEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public Consumer setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
        return this;
    }

    public Consumer setCounterGroup(CounterGroup counterGroup) {
        this.counterGroup = counterGroup;
        return this;
    }

    public Consumer setSourceCounter(SourceCounter sourceCounter) {
        this.sourceCounter = sourceCounter;
        return this;
    }

    public Consumer setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
        return this;
    }

    public Consumer setUsername(String username) {
        this.username = username;
        return this;
    }

    public Consumer setPassword(String password) {
        this.password = password;
        return this;
    }

    public Consumer setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public Consumer setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
        return this;
    }

    public Consumer setRequeing(boolean requeuing) {
        this.requeuing = requeuing;
        return this;
    }

    public Consumer setPrefetchCount(int prefetchCount) {
        this.prefetchCount = prefetchCount;
        return this;
    }

    public Consumer setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public void run() {
        DefaultConsumer consumer;
        Config factory = new Config();

        // Creating RabbitMQ connection
        try {
            connection = createRabbitMQConnection(factory);
        } catch (IOException ex) {
            logger.error("Error while creating RabbitMQ connection: {}", ex);
            return;
        }
        // Keep track of opened connections
        sourceCounter.setOpenConnectionCount(sourceCounter.getOpenConnectionCount() + 1);
        // Open a channel for the created RabbitMQ connection
        try {
            channel = connection.createChannel();
        } catch (IOException ex) {
            logger.error("Error while creating RabbitMQ channel: {}", ex);
            return;
        }
        // Set QoS pre-fetching if it is enabled. And close the connection if fails to set the QoS pre-fetching
        if (prefetchCount > 0) {
            if (!setQoS()) {
                this.close();
                return;
            }
        }
        // Create the new consumer and set the consumer tag
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                sourceCounter.incrementEventReceivedCount();
                try {
                    channelProcessor.processEvent(createFlumeEvent(envelope, properties, body));
                    sourceCounter.incrementEventAcceptedCount();
                    //Acknowledge the received message
                    ackMessage(envelope.getDeliveryTag());
                } catch (Exception ex) {
                    logger.error("Error while writing to channel for {}, message rejected {}", this, ex);
                    // Reject the message
                    rejectMessage(envelope.getDeliveryTag());
                }
            }
        };
        try {
            channel.basicConsume(queue, autoAck, Constants.Consumer.CONSUMER_TAG, consumer);
        } catch (IOException ex) {
            logger.error("Error while starting the consumer: {}", ex);
            // Keep track of RabbitMQ exceptions for the source
            counterGroup.incrementAndGet(Constants.Consumer.COUNTER_EXCEPTION);
            this.close();
        }
    }

    /**
     * Acknowledge the message
     *
     * @param deliveryTag the deliveryTag property of envelop
     */
    private void ackMessage(long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (IOException ex) {
            logger.error("Error while acknowledging message from {}: {}", this, ex);
            // Keep track of RabbitMQ exceptions for the source
            counterGroup.incrementAndGet(Constants.Consumer.COUNTER_EXCEPTION);
        }
        // Keep track of RabbitMQ acknowledgements
        counterGroup.incrementAndGet(Constants.Consumer.COUNTER_ACK);
    }

    /**
     * Reject the message in case of failure
     *
     * @param deliveryTag the deliveryTag property of envelop
     */
    private void rejectMessage(long deliveryTag) {
        try {
            channel.basicReject(deliveryTag, requeuing);
        } catch (IOException ex) {
            logger.error("Error while rejecting message from {}: {}", this, ex);
            // Keep track of RabbitMQ exceptions
            counterGroup.incrementAndGet(Constants.Consumer.COUNTER_EXCEPTION);
        }
        // Keep track of RabbitMQ message rejections
        counterGroup.incrementAndGet(Constants.Consumer.COUNTER_REJECT);
    }

    /**
     * @param envelope encapsulated a group of parameters used for AMQP's Basic methods
     * @param props    AMQP's Basic properties
     * @param body     the Message
     * @return the flume event
     */
    private Event createFlumeEvent(Envelope envelope, AMQP.BasicProperties props, byte[] body) {
        // Create the event with the message body
        Event event = EventBuilder.withBody(body);
        // Get the headers from properties, exchange, and routing-key
        Map<String, String> headers = buildHeaders(props);
        String exchange = envelope.getExchange();
        String routingKey = envelope.getRoutingKey();

        if (StringUtils.isNotEmpty(exchange)) {
            headers.put(Constants.Consumer.EXCHANGE_KEY, exchange);
        }
        if (StringUtils.isNotEmpty(routingKey)) {
            headers.put(Constants.Consumer.ROUTING_KEY, routingKey);
        }
        event.setHeaders(headers);
        return event;
    }

    /**
     * Build a header map with AMQP basic properties.
     *
     * @param props AMQP basic properties
     * @return the map with header properties
     */
    private Map<String, String> buildHeaders(AMQP.BasicProperties props) {
        Map<String, String> headers = new HashMap<>();

        String appId = props.getAppId();
        String contentEncoding = props.getContentEncoding();
        String contentType = props.getContentType();
        String correlationId = props.getCorrelationId();
        Integer deliveryMode = props.getDeliveryMode();
        String expiration = props.getExpiration();
        String messageId = props.getMessageId();
        Integer priority = props.getPriority();
        String replyTo = props.getReplyTo();
        Date timestamp = props.getTimestamp();
        String type = props.getType();
        String userId = props.getUserId();

        if (StringUtils.isNotEmpty(appId)) {
            headers.put(Constants.Consumer.APP_ID, appId);
        }
        if (StringUtils.isNotEmpty(contentEncoding)) {
            headers.put(Constants.Consumer.CONTENT_ENCODING, contentEncoding);
        }
        if (StringUtils.isNotEmpty(contentType)) {
            headers.put(Constants.Consumer.CONTENT_TYPE, contentType);
        }
        if (StringUtils.isNotEmpty(correlationId)) {
            headers.put(Constants.Consumer.CORRELATION_ID, correlationId);
        }
        if (deliveryMode != null) {
            headers.put(Constants.Consumer.DELIVERY_MODE, String.valueOf(deliveryMode));
        }
        if (StringUtils.isNotEmpty(expiration)) {
            headers.put(Constants.Consumer.EXPIRATION, expiration);
        }
        if (StringUtils.isNotEmpty(messageId)) {
            headers.put(Constants.Consumer.MESSAGE_ID, messageId);
        }
        if (priority != null) {
            headers.put(Constants.Consumer.PRIORITY, String.valueOf(priority));
        }
        if (StringUtils.isNotEmpty(replyTo)) {
            headers.put(Constants.Consumer.REPLY_TO, replyTo);
        }
        if (timestamp != null) {
            headers.put(Constants.Consumer.TIMESTAMP, String.valueOf(timestamp.getTime()));
        }
        if (StringUtils.isNotEmpty(type)) {
            headers.put(Constants.Consumer.TYPE, type);
        }
        if (StringUtils.isNotEmpty(userId)) {
            headers.put(Constants.Consumer.USER_ID, userId);
        }

        Map<String, Object> userHeaders = props.getHeaders();
        if (userHeaders != null && userHeaders.size() > 0) {
            for (String key : userHeaders.keySet()) {
                Object value = userHeaders.get(key);
                if (value != null) {
                    headers.put(key, userHeaders.get(key).toString());
                } else {
                    // Keep the header just to use as a flag if needed.
                    headers.put(key, "");
                }
            }
        }

        return headers;
    }

    /**
     * Set the QoS pre-fetching
     *
     * @return the status
     */
    private boolean setQoS() {
        try {
            channel.basicQos(prefetchCount);
        } catch (IOException ex) {
            logger.error("Error while setting QoS pre-fetch count: {}", ex);
            return false;
        }
        return true;
    }

    /**
     * Shutdown the consumer
     */
    public void shutdown() {
        // Tell RabbitMQ that the consumer is stopping
        try {
            channel.basicCancel(Constants.Consumer.CONSUMER_TAG);
        } catch (IOException ex) {
            logger.error("Error while cancelling the consumer for {}: {}", this, ex);
            counterGroup.incrementAndGet(Constants.Consumer.COUNTER_EXCEPTION);
        }

        // Close the consumer
        this.close();
    }

    /**
     * Close the channel and RabbitMQ connection
     */
    private void close() {
        try {
            channel.close();
            connection.close();
        } catch (IOException ex) {
            logger.error("Error while closing the RabbitMQ connection: {}", ex.toString());
        }
    }

    /**
     * Create the RabbitMQ connection.
     *
     * @param config the connection configuration
     * @return the connection to the RabbitMQ
     * @throws IOException if an error is encountered
     */
    private Connection createRabbitMQConnection(Config config) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Connecting to RabbitMQ from {}", this);
        }
        config = config.withRecoveryPolicy(RecoveryPolicies.recoverAlways())
                .withRetryPolicy(new RetryPolicy()
                        .withMaxAttempts(200)
                        .withInterval(Duration.seconds(1))
                        .withMaxDuration(Duration.minutes(5)));
        ConnectionOptions options = new ConnectionOptions()
                .withHost(hostname)
                .withPort(port)
                .withVirtualHost(virtualHost)
                .withUsername(username)
                .withPassword(password);
        if (sslEnabled) {
            try {
                options = options.withSsl();
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                logger.error("Could not enable SSL: {}", e.toString());
            }
        }
        try {
            return Connections.create(options, config);
        } catch (TimeoutException e) {
            logger.error("Timeout connecting to RabbitMQ: {}", e.toString());
            throw new IOException();
        }
    }
}
