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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.rabbitmq.client.util.Constants;
import org.wso2.rabbitmq.client.util.RabbitMQUtil;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;


public class RabbitMQSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQSource.class);

    private ConnectionFactory factory;
    private SourceCounter sourceCounter;
    private CounterGroup counterGroup;
    private String hostname;
    private int port;
    private String virtualHost;
    private String username;
    private String password;
    private String queue;
    private boolean enableSSL = false;
    private boolean autoAck = false;
    private boolean requeuing = false;
    private int prefetchCount = 0;
    private int timeout = -1;
    private int consumerThreads = 1;

    private List<Consumer> consumers;

    public RabbitMQSource() {
        this(new ConnectionFactory());
    }

    public RabbitMQSource(ConnectionFactory factory) {
        consumers = new LinkedList<>();
        this.factory = factory;
    }

    @Override
    public void configure(Context context) {
        // Ensuring the queue name does not null
        Configurables.ensureRequiredNonNull(context, Constants.Consumer.QUEUE_KEY);
        // Load all of the configuration properties
        loadConsumerProperties(context);
        // Ensure that Flume can connect to the configured RabbitMQ broker
        checkRabbitMQConnection();
        // Create and configure the counters
        sourceCounter = new SourceCounter(getName());
        counterGroup = new CounterGroup();
        counterGroup.setName(getName());
    }

    @Override
    public synchronized void start() {
        logger.info("Starting the source {} with {} thread(s)", getName(), consumerThreads);
        sourceCounter.start();
        for (int i = 0; i < consumerThreads; i++) {
            Consumer consumer = new Consumer()
                    .setHostname(hostname)
                    .setPort(port)
                    .setSSLEnabled(enableSSL)
                    .setVirtualHost(virtualHost)
                    .setUsername(username)
                    .setPassword(password)
                    .setQueue(queue)
                    .setPrefetchCount(prefetchCount)
                    .setTimeout(timeout)
                    .setAutoAck(autoAck)
                    .setRequeing(requeuing)
                    .setChannelProcessor(getChannelProcessor())
                    .setSourceCounter(sourceCounter)
                    .setCounterGroup(counterGroup);
            Thread thread = new Thread(consumer);
            thread.setName("RabbitMQ Consumer #" + String.valueOf(i));
            thread.start();
            consumers.add(consumer);
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stopping the source {}", getName());
        for (int i = 0; i < consumerThreads; i++) {
            if (logger.isDebugEnabled()) {
                logger.debug("Stopping consumer thread #{}", i);
            }
            Consumer consumer = consumers.get(i);
            consumer.shutdown();
        }
        sourceCounter.stop();
        super.stop();
    }

    /**
     * Load all configured properties
     *
     * @param context
     */
    private void loadConsumerProperties(Context context) {
        hostname = context.getString(Constants.Consumer.HOST_KEY, ConnectionFactory.DEFAULT_HOST);
        port = context.getInteger(Constants.Consumer.PORT_KEY, ConnectionFactory.DEFAULT_AMQP_PORT);
        enableSSL = context.getBoolean(Constants.Consumer.SSL_KEY, false);
        virtualHost = context.getString(Constants.Consumer.VHOST_KEY, ConnectionFactory.DEFAULT_VHOST);
        username = context.getString(Constants.Consumer.USER_KEY, ConnectionFactory.DEFAULT_USER);
        password = context.getString(Constants.Consumer.PASSWORD_KEY, ConnectionFactory.DEFAULT_PASS);
        queue = context.getString(Constants.Consumer.QUEUE_KEY, null);
        autoAck = context.getBoolean(Constants.Consumer.AUTOACK_KEY, false);
        requeuing = context.getBoolean(Constants.Consumer.REQUEUING, false);
        prefetchCount = context.getInteger(Constants.Consumer.PREFETCH_COUNT_KEY, 0);
        timeout = context.getInteger(Constants.Consumer.TIMEOUT_KEY, -1);
        consumerThreads = context.getInteger(Constants.Consumer.THREAD_COUNT_KEY, 1);
    }

    /**
     * Create a connection with the RabbitMQ broker
     */
    private void checkRabbitMQConnection() {
        Connection connection = null;

        factory = RabbitMQUtil.setFactoryProperties(factory, hostname, port, virtualHost, username, password);
        if (enableSSL) {
            try {
                factory.useSslProtocol();
            } catch (NoSuchAlgorithmException | KeyManagementException ex) {
                throw new IllegalArgumentException("Could not Enable SSL: " + ex.toString());
            }
        }
        try {
            connection = factory.newConnection();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Could not connect to RabbitMQ: " + ex.toString());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException ex) {
                    throw new IllegalArgumentException("Could not close the RabbitMQ test connection: "
                            + ex.toString());
                }
            }
        }
    }
}
