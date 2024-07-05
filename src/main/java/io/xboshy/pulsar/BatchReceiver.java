package io.xboshy.pulsar;

import io.xboshy.pulsar.impl.MessagesImpl;
import io.prometheus.client.Summary;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import java.util.concurrent.TimeUnit;

public class BatchReceiver<T> {
    final Consumer<T> consumer;
    final BatchReceivePolicy batchReceivePolicy;
    private static final Summary messageWaitTime = Summary.build()
            .name("message_wait_time")
            .help("message_wait_time")
            .quantile(0.50, 0.005)
            .quantile(0.80, 0.005)
            .quantile(0.90, 0.005)
            .quantile(0.95, 0.005)
            .quantile(0.99, 0.005)
            .register();

    public BatchReceiver(Consumer<T> consumer, BatchReceivePolicy batchReceivePolicy) {
        this.consumer = consumer;
        this.batchReceivePolicy = batchReceivePolicy;
    }

    public MessagesImpl<T> batchReceive(final Consumer<T> consumer, final MessagesImpl<T> messages) throws Exception {
        final long timeoutConf = this.batchReceivePolicy.getTimeoutMs();
        final MessagesImpl<T> overflowMessages = new MessagesImpl<>(
                this.batchReceivePolicy.getMaxNumMessages(),
                this.batchReceivePolicy.getMaxNumBytes()
        );

        Message<T> msg;
        if (messages.size() == 0) {
            try (final Summary.Timer ignored = BatchReceiver.messageWaitTime.startTimer()) {
                msg = consumer.receive();
            }
            messages.add(msg);
        }

        boolean full = false;
        long age = messages.age();
        while (!full) {
            final int delta = (int) (age >= timeoutConf ? 0 : timeoutConf - age);

            try (final Summary.Timer ignored = BatchReceiver.messageWaitTime.startTimer()) {
                msg = consumer.receive(delta, TimeUnit.MILLISECONDS);
            }
            if (msg == null) {
                break;
            }

            final boolean canAdd = messages.canAdd(msg);
            if (canAdd) {
                messages.add(msg);
            } else {
                overflowMessages.add(msg);
            }

            age = messages.age();
            full = !canAdd || age >= timeoutConf;
        }

        return overflowMessages;
    }
}
