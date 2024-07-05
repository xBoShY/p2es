package io.xboshy.pulsar;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.Base64;

public class Id {
    public static String messageIdToElasticId(final MessageId msgId) {
        return Base64.getEncoder().encodeToString(msgId.toByteArray());
    }

    public static MessageId elasticIdToPulsarId(final String msgIdStr) throws Exception {
        return MessageId.fromByteArray(Base64.getDecoder().decode(msgIdStr));
    }

    public static MessageId messageIdBatchFix(final MessageId msgId) {
        if (msgId instanceof BatchMessageIdImpl batchMessageId) {
            /* Issue 6869 */
            return new MessageIdImpl(
                            batchMessageId.getLedgerId(),
                            batchMessageId.getEntryId(),
                            batchMessageId.getPartitionIndex()
                    );
        }

        return msgId;
    }
}
