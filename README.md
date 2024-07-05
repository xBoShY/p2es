p2es is an adapter that consumer messages from Pulsar and produces them to Elasticsearch.

## Build docker images

This builds the docker image `io.xboshy.pulsar/p2es:2.0.0` using a multi-stage build

```bash
docker build -t io.xboshy.pulsar/p2es:2.0.0 .
```

Launching a container

```bash
docker run --rm -it \
  --name p2es-topicname-subscriptionname \
  --net dockernet \
  \
  -e PROMETHEUS_port=8081 \
  \
  -e GLOBAL_inflightBatches=4 \
  \
  -e ELASTICSEARCH_hosts=http://es01:9200,http://es02:9200 \
  -e ELASTICSEARCH_indexName=indexname \
  -e ELASTICSEARCH_ioThreads=4 \
  -e ELASTICSEARCH_password=mypass \
  -e ELASTICSEARCH_username=elastic \
  \
  -e PULSAR_CLIENT_serviceUrl=pulsar://pulsar:6650 \
  -e PULSAR_CLIENT_numIoThreads=1 \
  -e PULSAR_CLIENT_numListenerThreads=1 \
  -e PULSAR_CLIENT_connectionsPerBroker=1 \
  -e PULSAR_CLIENT_useTcpNoDelay=true \
  \
  -e PULSAR_CONSUMER_batchReceivePolicy.maxNumBytes=1638400 \
  -e PULSAR_CONSUMER_batchReceivePolicy.maxNumMessages=1000 \
  -e PULSAR_CONSUMER_batchReceivePolicy.timeout=100 \
  -e PULSAR_CONSUMER_acknowledgementsGroupTimeMicros=5000 \
  -e PULSAR_CONSUMER_ackTimeoutMillis=1000 \
  -e PULSAR_CONSUMER_consumerName=consumer \
  -e PULSAR_CONSUMER_receiverQueueSize=10000 \
  -e PULSAR_CONSUMER_subscriptionName=subs \
  -e PULSAR_CONSUMER_subscriptionType=Shared \
  -e PULSAR_CONSUMER_topicNames=persistent://public/default/topicname \
  \
  io.xboshy.pulsar/p2es:2.0.0
```
