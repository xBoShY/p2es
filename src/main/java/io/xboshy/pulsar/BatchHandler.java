package io.xboshy.pulsar;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import io.xboshy.pulsar.config.GlobalConfig;
import io.xboshy.pulsar.utils.Hashutils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import jakarta.json.stream.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.pulsar.client.api.*;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Predicate;

public class BatchHandler<T> implements EventHandler<Batch<T>>, WorkHandler<Batch<T>> {
    private final Schema<T> schema;
    private final String clusterName;
    private final String endpoint;
    private final Consumer<T> consumer;
    private final RestClient producer;
    private final JsonpMapper mapper;
    private final Function<Message<T>, String> idFunction;
    private final MessageDigest sha1;
    private static final Summary batchProcessing = Summary.build()
            .name("batch_processing_time")
            .help("batch_processing_time")
            .quantile(0.50, 0.005)
            .quantile(0.80, 0.005)
            .quantile(0.90, 0.005)
            .quantile(0.95, 0.005)
            .quantile(0.99, 0.005)
            .register();
    private static final Summary ackingTime = Summary.build()
            .name("acking_time")
            .help("acking_time")
            .quantile(0.50, 0.005)
            .quantile(0.80, 0.005)
            .quantile(0.90, 0.005)
            .quantile(0.95, 0.005)
            .quantile(0.99, 0.005)
            .register();
    private static final Counter unrolledBatches = Counter.build()
            .name("unrolled_batches")
            .help("unrolled_batches")
            .register();
    private static final Counter eventsOk = Counter.build()
            .name("events_ok")
            .help("events_ok")
            .register();
    private static final Counter eventsKo = Counter.build()
            .name("events_ko")
            .help("events_ko")
            .register();
    private static final Counter elasticsearchEventsResult = Counter.build()
            .name("elasticsearch_events")
            .help("elasticsearch_events")
            .labelNames("status", "reason")
            .register();
    private static final Counter elasticsearchBatchResult = Counter.build()
            .name("elasticsearch_batch")
            .help("elasticsearch_batch")
            .labelNames("status", "reason")
            .register();

    private static final Predicate<BulkResponseItem> okRule = (item) -> item.error() == null || (item.error().type() != null && item.error().type().equals("version_conflict_engine_exception"));

    public BatchHandler(GlobalConfig.IdModes idMode, Schema<T> schema, String clusterName, String endpoint, Consumer<T> consumer, RestClient producer) throws Exception {
        this.schema = schema;
        this.clusterName = clusterName;
        this.endpoint = endpoint;
        this.consumer = consumer;
        this.mapper = new JacksonJsonpMapper();
        this.producer = producer;
        this.sha1 = Hashutils.getMessageDigestSHA1();

        this.idFunction = switch(idMode) {
            case NONE -> msg -> null;
            case MSGID -> msg -> {
                final String msgId = Id.messageIdToElasticId(msg.getMessageId());
                final String ts = Long.toString(msg.getPublishTime());
                return String.format("%s-%s", msgId, ts);
            };
            case HASH -> msg -> {
                if (msg.getData() == null) {
                    return "";
                }
                return Hashutils.SHA1(this.sha1, msg.getData());
            };
            case KEY -> msg -> {
                final String key = msg.getKey();
                if (key == null || key.isEmpty()) {
                    return null;
                }
                return key;
            };
            case KEYHASH -> msg -> {
                final String key = msg.getKey();
                if (key == null || key.isEmpty()) {
                    return null;
                }
                if (msg.getData() == null) {
                    return String.format("%s", key);
                }
                String hash = Hashutils.SHA1(this.sha1, msg.getData());
                return String.format("%s-%s", key, hash);
            };
        };
    }

    @Override
    public void onEvent(Batch<T> event, long sequence, boolean endOfBatch) throws Exception {
        this.onEvent(event);
    }

    @Override
    public void onEvent(Batch<T> event) throws Exception {
        final Messages<T> msgs = event.getMessages();
        try {
            if (msgs.size() <= 0) {
                return;
            }

            try (Summary.Timer ignored = BatchHandler.batchProcessing.startTimer()) {
                final HashMap<String, MessageId> idMap = new HashMap<>();
                final ArrayList<MessageId> nullIDsList = new ArrayList<>();
                final StringBuilder bulkRequestBody = new StringBuilder();
                for (final Message<T> msg : msgs) {
                    final String _id = this.idFunction.apply(msg);
                    final String actionMetadata;
                    if (_id != null) {
                        if (idMap.containsKey(_id)) {
                            this.consumer.acknowledge(msg);
                            continue;
                        }
                        idMap.put(_id, msg.getMessageId());
                        actionMetadata = String.format("{ \"create\": { \"_id\": \"%s\" } }%n", _id);
                    } else {
                        nullIDsList.add(msg.getMessageId());
                        actionMetadata = String.format("{ \"create\": { } }%n");
                    }

                    final String bulkItem = new String(this.schema.encode(msg.getValue()));
                    bulkRequestBody.append(actionMetadata);
                    bulkRequestBody.append(bulkItem);
                    bulkRequestBody.append("\n");
                }

                final HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
                final Request request = new Request("PUT", this.endpoint);
                request.setEntity(entity);

                final Response response;
                response = this.producer.performRequest(request);
                final int status = response.getStatusLine().getStatusCode();

                BatchHandler.elasticsearchBatchResult.labels(
                        String.valueOf(status),
                        response.getStatusLine().getReasonPhrase()
                ).inc();

                if (status == HttpStatus.SC_OK) {
                    final JsonParser parser = this.mapper.jsonProvider().createParser(response.getEntity().getContent());
                    final BulkResponse bResp = BulkResponse._DESERIALIZER.deserialize(parser, this.mapper);

                    try (Summary.Timer ignored1 = BatchHandler.ackingTime.startTimer()) {
                        boolean hasErrors = false;
                        for (BulkResponseItem item : bResp.items()) {
                            if (!hasErrors && !BatchHandler.okRule.test(item)) {
                                hasErrors = true;
                            }
                            BatchHandler.elasticsearchEventsResult.labels(
                                    String.valueOf(item.status()),
                                    item.error() == null ? "" : item.error().type()
                            ).inc();
                        }

                        if (!hasErrors && !bResp.errors()) {
                            this.consumer.acknowledge(event.getMessages());
                            BatchHandler.eventsOk.inc(event.getMessages().size());
                        } else {
                            BatchHandler.unrolledBatches.inc();
                            int nullid = 0;
                            for (BulkResponseItem item : bResp.items()) {
                                MessageId id;
                                if (idMap.containsKey(item.id())) {
                                    id = idMap.get(item.id());
                                } else {
                                    id = nullIDsList.get(nullid);
                                    ++nullid;
                                }

                                if (BatchHandler.okRule.test(item)) {
                                    this.consumer.acknowledge(id);
                                    BatchHandler.eventsOk.inc();
                                } else {
                                    this.consumer.negativeAcknowledge(id);
                                    MessageId idB = Id.messageIdBatchFix(id);
                                    if (!id.equals(idB)) {
                                        this.consumer.negativeAcknowledge(idB);
                                    }
                                    BatchHandler.eventsKo.inc();
                                }
                            }
                        }
                    }
                } else {
                    throw new Exception("Bad response: " + response.getStatusLine().getStatusCode());
                }
            }
        } finally {
            event.clear();
        }
    }
}
