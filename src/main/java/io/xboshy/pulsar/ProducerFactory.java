package io.xboshy.pulsar;

import io.xboshy.pulsar.config.ElasticsearchConfig;
import io.xboshy.pulsar.config.GlobalConfig;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;

public class ProducerFactory {
    private RestClientBuilder builder;

    public ProducerFactory(final GlobalConfig globalConfig, final ElasticsearchConfig elasticsearchConfig) throws Exception {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(elasticsearchConfig.getUsername(), elasticsearchConfig.getPassword())
        );

        final SSLContext sslContext = elasticsearchConfig.getSslContext();
        this.builder = RestClient.builder(
                elasticsearchConfig.getHosts()
        )
                .setHttpClientConfigCallback(
                        httpClientBuilder -> {
                            if (elasticsearchConfig.hasAuth()) {
                                httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider);
                            }
                            httpClientBuilder
                                .setDefaultIOReactorConfig(
                                        IOReactorConfig.custom()
                                                .setIoThreadCount(elasticsearchConfig.getIoThreads())
                                                .build()
                                );
                            if (sslContext != null) {
                                httpClientBuilder.setSSLContext(sslContext);
                            }
                            return httpClientBuilder;
                        }
                )
                .setRequestConfigCallback(
                        requestConfigBuilder -> {
                            requestConfigBuilder
                                    .setSocketTimeout(elasticsearchConfig.getSocketTimeout())
                                    .setContentCompressionEnabled(elasticsearchConfig.getContentCompressionEnabled())
                                    .setConnectTimeout(elasticsearchConfig.getConnectTimeout())
                                    .setConnectionRequestTimeout(elasticsearchConfig.getConnectionRequestTimeout())
                                    .setAuthenticationEnabled(elasticsearchConfig.hasAuth());
                            return requestConfigBuilder;
                        }
                )
                .setCompressionEnabled(true);
    }

    public RestClient getClient() {
        return this.builder.build();
    }

    public void close() {
        try {
            this.builder = null;
        } catch (Exception e) {
            /* skip */
        }
    }
}
