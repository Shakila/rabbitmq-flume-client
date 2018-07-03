rabbitmq-flume-client
=====================
A Flume plugin that provides a RabbitMQ *Source* for Flume events.

Tested Platform
--------------- 

- Mac OSx 10.13.5
- Java 1.8
- Flume 1.8.0

Installation Instructions
-------------------------

- To install this client to the Flume, copy the client jar file (*rabbitmq-flume-client*) to the ``<FLUME_HOME>/lib`` directory.
- Create the configuration file (*wso2mqagent.conf*) into ``<FLUME_HOME>/conf`` directory with the configuration properties. Please refer the *Sample Configuration*.
- Start the WSO2 MB and create a queue (*test_queue*) from the management console.

Configuration
-------------
Here we look at the RabbitMQ configuration parameters.

### Source
The RabbitMQ Source component enables RabbitMQ to act as a source for Flume events.
When the Source consumes a message, all message properties that are set on the message
will be added to the Flume ``Event`` headers.

By default, there is a single consumer thread in the RabbitMQ source.

The Source component has the following configuration properties:

Variable          | Default       | Description
----------------- | ------------- | -----------
host              | ``localhost`` | The RabbitMQ host to connect to
port              | ``5672``      | The port to connect on
ssl               | ``false``     | Connect to RabbitMQ via SSL
virtual-host      | ``/carbon``   | The virtual host name to connect to
username          | ``admin``     | The username to connect as
password          | ``admin``     | The password to use when connecting
queue             |               | **Required** field specifying the name of the queue to consume from
auto-ack          | ``false``     | Enable auto-acknowledgement for higher throughput with the chance of message loss
requeuing         | ``false``     | Instruct the broker to discard or requeue failed (rejected) messages
prefetchCount     | ``0``         | The ``Basic.QoS`` prefetch count to specify for consuming
timeout           | ``-1``        | The timeout the consumer will wait for rabbitmq to deliver a message before retrying
threads           | ``1``         | The number of consumer threads to create

#### Possible event header properties

- exchange
- routing-key
- app-id
- content-encoding
- content-type
- correlation-id
- delivery-mode
- expires
- message-id
- priority
- reply-to
- timestamp
- type
- user-id

Sample Configuration
--------------------
Wso2Agent.sources = rabbitmq_source1
Wso2Agent.channels = file_channel

#Define RabbitMQ Source<br/>
Wso2Agent.sources.rabbitmq_source1.type = org.wso2.rabbitmq.client.source.RabbitMQSource<br/>
Wso2Agent.sources.rabbitmq_source1.host = localhost<br/>
Wso2Agent.sources.rabbitmq_source1.queue = test_queue<br/>
Wso2Agent.sources.rabbitmq_source1.username = admin<br/>
Wso2Agent.sources.rabbitmq_source1.password = admin<br/>
Wso2Agent.sources.rabbitmq_source1.port = 5672<br/>
Wso2Agent.sources.rabbitmq_source1.virtual-host = /carbon

#Use a channel which buffers events in memory<br/>
Wso2Agent.channels.file_channel.type = file<br/>
Wso2Agent.channels.file_channel.checkpointDir = <FLUME_HOME>/checkpoint<br/>
Wso2Agent.channels.file_channel.dataDirs = <FLUME_HOME>/data

#Bind the source to the channel<br/>
Wso2Agent.sources.rabbitmq_source1.channels = file_channel

Consume Messages From WSO2 MB
-----------------------------
Run the following command to consume the messages from test_queue.
./bin/flume-ng agent -c conf -f conf/wso2mqagent.conf -n Wso2Agent -Dflume.root.logger=DEBUG,console

Build Client
------------
To build the client jar from source, use ``maven`` build tool:

Since we don't need to install the package into the local repository,
```bash
mvn package
```

This will download all the dependencies and provide the client **jar** file in the ``target/`` directory.
