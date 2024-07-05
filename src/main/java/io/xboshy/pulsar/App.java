package io.xboshy.pulsar;

import io.xboshy.pulsar.config.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.elasticsearch.client.RestClient;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class App implements Runnable {
    private static final Logger logger = LogManager.getLogger(App.class);
    private static final Schema<byte[]> SCHEMA = Schema.BYTES;
    private static final EventFactory<Batch<byte[]>> EVENT_FACTORY = Batch::new;
    private final GlobalConfig globalConfig;
    private final PulsarClientConfig pulsarClientConfig;
    private final PulsarConsumerConfig pulsarConsumerConfig;
    private final ElasticsearchConfig elasticsearchConfig;
    private final ConsumerFactory<byte[]> consumerFactory;
    private final ProducerFactory producerFactory;
    private final Disruptor<Batch<byte[]>> disruptor;
    private final AtomicBoolean error;
    private static final Summary dispatchWaitTime = Summary.build()
            .name("dispatch_wait_time_millis")
            .help("dispatch_wait_time_millis")
            .quantile(0.50, 0.005)
            .quantile(0.80, 0.005)
            .quantile(0.90, 0.005)
            .quantile(0.95, 0.005)
            .quantile(0.99, 0.005)
            .register();

    public App(final Map<String, String> config) throws Exception {
        this.error = new AtomicBoolean(false);
        this.globalConfig = new GlobalConfig(config);
        this.pulsarClientConfig = new PulsarClientConfig(config);
        this.pulsarConsumerConfig = new PulsarConsumerConfig(config);
        this.elasticsearchConfig = new ElasticsearchConfig(config);

        this.consumerFactory = new ConsumerFactory<>(this.globalConfig, this.pulsarClientConfig);
        this.producerFactory = new ProducerFactory(this.globalConfig, this.elasticsearchConfig);

        final ThreadFactory threadFactory = new CustomThreadFactory("disruptor-worker");
        final WaitStrategy waitStrategy = new BlockingWaitStrategy();
        this.disruptor = new Disruptor<>(
                App.EVENT_FACTORY,
                this.globalConfig.getRingBuffer(),
                threadFactory,
                ProducerType.SINGLE,
                waitStrategy
        );
    }

    public void run() {
        RestClient producerRef = null;
        Consumer<byte[]> consumerRef = null;
        try {
            final RestClient producer = this.producerFactory.getClient();
            producerRef = producer;

            final Consumer<byte[]> consumer = this.consumerFactory.getConsumer(this.globalConfig, this.pulsarConsumerConfig, App.SCHEMA);
            consumerRef = consumer;

            final String indexName = this.elasticsearchConfig.getIndexName();
            final String endpoint = String.format("%s/_bulk", indexName);

            final int nWorkers = this.globalConfig.getInflightBatches();
            final WorkHandler<Batch<byte[]>>[] workers = new WorkHandler[nWorkers];
            final String clusterName = this.pulsarClientConfig.getClusterName();
            for (int i = 0; i < nWorkers; ++i) {
                final WorkHandler<Batch<byte[]>> worker = new BatchHandler<>(this.globalConfig.getIdMode(), App.SCHEMA, clusterName, endpoint, consumer, producer);
                workers[i] = worker;
            }
            final Thread appThread = Thread.currentThread();
            this.disruptor.handleEventsWithWorkerPool(workers);
            this.disruptor.setDefaultExceptionHandler(new ExceptionHandler<>() {
                @Override
                public void handleEventException(Throwable e, long l, Batch<byte[]> batch) {
                    App.logger.warn("exception caught", e);
                    e.printStackTrace();
                    error.set(true);
                    //appThread.interrupt();
                }

                @Override
                public void handleOnStartException(Throwable e) {
                    App.logger.warn("exception caught", e);
                    e.printStackTrace();
                    error.set(true);
                    //appThread.interrupt();
                }

                @Override
                public void handleOnShutdownException(Throwable e) {
                    App.logger.warn("exception caught", e);
                    e.printStackTrace();
                    error.set(true);
                    //appThread.interrupt();
                }
            });
            final RingBuffer<Batch<byte[]>> ringBuffer = this.disruptor.start();

            while (!this.error.get()) {
                final long seq;
                final Batch<byte[]> batch;
                try (final Summary.Timer ignored = App.dispatchWaitTime.startTimer()) {
                    seq = ringBuffer.next();
                    batch = ringBuffer.get(seq);
                }

                Messages<byte[]> msgs = null;
                String logmsg = String.format("{\"step\": \"getbatch\", \"sequence\": %d}", seq);
                App.logger.info(logmsg);
                while (msgs == null || msgs.size() == 0) {
                    msgs = consumer.batchReceive();
                }
                logmsg = String.format("{\"step\": \"batchreceived\", \"sequence\": %d, \"msgs\": %d}", seq, msgs.size());
                App.logger.info(logmsg);

                batch.setMessages(msgs);
                ringBuffer.publish(seq);
            }
        } catch (Exception e) {
            App.logger.error("exception caught", e);
        } finally {
            if (consumerRef != null) {
                try {
                    App.logger.info("closing consumer...");
                    consumerRef.close();
                    App.logger.info("consumer closed");
                } catch (Exception e) {
                    /* skip */
                }
            }
            if (producerRef != null) {
                try {
                    App.logger.info("closing producer...");
                    producerRef.close();
                    App.logger.info("producer closed");
                } catch (Exception e) {
                    /* skip */
                }
            }
        }
    }

    public void close() {
        if (this.producerFactory != null) {
            this.producerFactory.close();
        }
        if (this.consumerFactory != null) {
            this.consumerFactory.close();
        }
        this.disruptor.shutdown();
    }

    public static void main(final String[] args) {
        App app = null;
        final PrometheusConfig prometheusConfig = new PrometheusConfig(System.getenv());

        AtomicReference<HTTPServer> httpServer = new AtomicReference<>();
        boolean dieFast = false;
        try {
            App.logger.info("creating app");
            DefaultExports.initialize();
            if (prometheusConfig.getPort() != 0) {
                httpServer.set(new HTTPServer(prometheusConfig.getPort(), true));
            }
            app = new App(System.getenv());
            App.logger.info("starting app");
            app.run();
        } catch (Exception e) {
            App.logger.error("app stopped", e);
            dieFast = true;
        } finally {
            App.logger.info("closing app");

            if (dieFast) {
                App.logger.error("shiiiiet, i r dead");
                System.exit(19840206);
            }
            if (httpServer.get() != null) {
                httpServer.get().close();
            }
            if (app != null) {
                app.close();
            }
        }
        App.logger.info("bye");
    }
}
