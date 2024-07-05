package io.xboshy.pulsar.config;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.DeadLetterPolicy.DeadLetterPolicyBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PulsarConsumerConfig extends Config {
    DeadLetterPolicyBuilder deadLetterPolicyBuilder = null;
    BatchReceivePolicy.Builder batchReceivePolicyBuilder = null;

    public PulsarConsumerConfig(final Map<String, String> config) {
        super(config);

        final String topicNamesString = this.getStrValue("topicNames");
        final String[] topicNamesArray = topicNamesString.split(",");
        final HashSet<String> topicNames = new HashSet<>();
        for (String topicName : topicNamesArray) {
            topicNames.add(topicName.trim());
        }
        this.config.put("topicNames", topicNames);

        final String batchReceivePolicy_maxNumMessages = this.getStrValue("batchReceivePolicy.maxNumMessages");
        final String batchReceivePolicy_maxNumBytes = this.getStrValue("batchReceivePolicy.maxNumBytes");
        final String batchReceivePolicy_timeout = this.getStrValue("batchReceivePolicy.timeout");
        if (batchReceivePolicy_maxNumMessages != null
            || batchReceivePolicy_maxNumBytes != null
            || batchReceivePolicy_timeout != null) {
            this.config.remove("batchReceivePolicy.maxNumMessages");
            this.config.remove("batchReceivePolicy.maxNumBytes");
            this.config.remove("batchReceivePolicy.timeout");

            this.batchReceivePolicyBuilder = BatchReceivePolicy.builder();

            if (batchReceivePolicy_maxNumMessages != null) {
                batchReceivePolicyBuilder.maxNumMessages(Integer.parseInt(batchReceivePolicy_maxNumMessages));
            }
            if (batchReceivePolicy_maxNumBytes != null) {
                batchReceivePolicyBuilder.maxNumBytes(Integer.parseInt(batchReceivePolicy_maxNumBytes));
            }
            if (batchReceivePolicy_timeout != null) {
                batchReceivePolicyBuilder.timeout(Integer.parseInt(batchReceivePolicy_timeout), TimeUnit.MILLISECONDS);
            }
        }

        final String deadLetterPolicy_maxRedeliverCount = this.getStrValue("deadLetterPolicy.maxRedeliverCount");
        final String deadLetterPolicy_retryLetterTopic = this.getStrValue("deadLetterPolicy.retryLetterTopic");
        final String deadLetterPolicy_deadLetterTopic = this.getStrValue("deadLetterPolicy.deadLetterTopic");
        if (deadLetterPolicy_maxRedeliverCount != null
            || deadLetterPolicy_retryLetterTopic != null
            || deadLetterPolicy_deadLetterTopic != null) {
            this.config.remove("deadLetterPolicy.maxRedeliverCount");
            this.config.remove("deadLetterPolicy.retryLetterTopic");
            this.config.remove("deadLetterPolicy.deadLetterTopic");

            DeadLetterPolicyBuilder deadLetterPolicyBuilder = DeadLetterPolicy.builder();

            if (deadLetterPolicy_maxRedeliverCount !=  null) {
                deadLetterPolicyBuilder.maxRedeliverCount(Integer.parseInt(deadLetterPolicy_maxRedeliverCount));
            }
            if (deadLetterPolicy_retryLetterTopic != null) {
                deadLetterPolicyBuilder.retryLetterTopic(deadLetterPolicy_retryLetterTopic);
            }
            if (deadLetterPolicy_deadLetterTopic != null) {
                deadLetterPolicyBuilder.deadLetterTopic(deadLetterPolicy_deadLetterTopic);
            }

            //this.config.put("deadLetterPolicy", deadLetterPolicyBuilder.build());
            this.deadLetterPolicyBuilder = deadLetterPolicyBuilder;
        }
    }

    @Override
    public String getPrefix() {
        return "PULSAR_CONSUMER_";
    }

    public DeadLetterPolicy getDeadLetterPolicy() {
        if (this.deadLetterPolicyBuilder != null)
            return this.deadLetterPolicyBuilder.build();

        return null;
    }

    public BatchReceivePolicy getBatchReceivePolicy() {
        if (this.batchReceivePolicyBuilder != null)
            return this.batchReceivePolicyBuilder.build();

        return BatchReceivePolicy.DEFAULT_POLICY;
    }
}
