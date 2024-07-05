package io.xboshy.pulsar;

import io.xboshy.pulsar.config.GlobalConfig;
import io.xboshy.pulsar.config.PulsarClientConfig;
import io.xboshy.pulsar.config.PulsarConsumerConfig;
import org.apache.pulsar.client.api.*;

public class ConsumerFactory<T> {
    private final PulsarClient client;

    public ConsumerFactory(final GlobalConfig globalConfig, final PulsarClientConfig pulsarClientConfig) throws Exception {
        this.client = PulsarClient.builder()
                .loadConf(pulsarClientConfig.getConfig())
                .build();
    }

    public Consumer<T> getConsumer(GlobalConfig globalConfig, PulsarConsumerConfig pulsarConsumerConfig, Schema<T> schema) throws Exception {
        final ConsumerBuilder<T> consumerBuilder = this.client.newConsumer(schema)
                .loadConf(pulsarConsumerConfig.getConfig());

        final BatchReceivePolicy batchReceivePolicy = pulsarConsumerConfig.getBatchReceivePolicy();
        if (batchReceivePolicy != null) {
            consumerBuilder.batchReceivePolicy(batchReceivePolicy);
        }

        final DeadLetterPolicy deadLetterPolicy = pulsarConsumerConfig.getDeadLetterPolicy();
        if (deadLetterPolicy != null) {
            consumerBuilder.deadLetterPolicy(deadLetterPolicy);
        }

        return consumerBuilder.subscribe();
    }

    public void close() {
        try {
            this.client.close();
        } catch (Exception e) {
            /* skip */
        }
    }
}
