package io.xboshy.pulsar.config;

import org.apache.http.HttpHost;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Map;

public class ElasticsearchConfig extends Config {
    private HttpHost[] hosts = null;
    private String username = null;
    private String password = null;
    private Integer ioThreads = null;
    private String indexName = null;
    private Integer socketTimeout = null;
    private Boolean contentCompressionEnabled = null;
    private Integer connectTimeout = null;
    private Integer connectionRequestTimeout = null;
    private String tlsTrustCertsFilePath = null;
    private SSLContext sslContext = null;

    public ElasticsearchConfig(final Map<String, String> config) throws Exception {
        super(config);
    }

    @Override
    public String getPrefix() {
        return "ELASTICSEARCH_";
    }

    public HttpHost[] getHosts() {
        if (this.hosts != null)
            return this.hosts;

        final String hostsStr = this.getStrValue("hosts", "localhost:9200");
        final String[] hostStrArr = hostsStr.split(",");
        this.hosts = new HttpHost[hostStrArr.length];
        for (int i = 0; i < hostStrArr.length; ++i) {
            this.hosts[i] = HttpHost.create(hostStrArr[i]);
        }

        return this.hosts;
    }

    public boolean hasAuth() {
        return this.getUsername() != null;
    }

    public String getUsername() {
        if (this.username != null)
            return this.username;

        this.username = this.getStrValue("username", null);
        return this.username;
    }

    public String getPassword() {
        if (this.password != null)
            return this.password;

        this.password = this.getStrValue("password", null);
        return this.password;
    }

    public int getIoThreads() {
        if (this.ioThreads != null)
            return this.ioThreads;

        this.ioThreads = this.getIntValue("ioThreads", 1);
        return this.ioThreads;
    }

    public String getIndexName() {
        if (this.indexName != null)
            return this.indexName;

        this.indexName = this.getStrValue("indexName", null);

        return this.indexName;
    }

    public int getSocketTimeout() {
        if (this.socketTimeout != null)
            return this.socketTimeout;

        this.socketTimeout = this.getIntValue("socketTimeout", 10000);

        return this.socketTimeout;
    }

    public boolean getContentCompressionEnabled() {
        if (this.contentCompressionEnabled != null)
            return this.contentCompressionEnabled;

        this.contentCompressionEnabled = this.getBoolValue("contentCompressionEnabled", true);

        return this.contentCompressionEnabled;
    }

    public int getConnectTimeout() {
        if (this.connectTimeout != null)
            return this.connectTimeout;

        this.connectTimeout = this.getIntValue("connectTimeout", 1000);

        return this.connectTimeout;
    }

    public int getConnectionRequestTimeout() {
        if (this.connectionRequestTimeout != null)
            return this.connectionRequestTimeout;

        this.connectionRequestTimeout = this.getIntValue("connectionRequestTimeout", 1000);

        return this.connectionRequestTimeout;
    }

    public String getTlsTrustCertsFilePath() {
        if (this.tlsTrustCertsFilePath != null)
            return this.tlsTrustCertsFilePath;

        this.tlsTrustCertsFilePath = this.getStrValue("tlsTrustCertsFilePath");

        return this.tlsTrustCertsFilePath;
    }

    public SSLContext getSslContext() throws Exception {
        if (this.sslContext != null) {
            return this.sslContext;
        }

        final String tlsTrustCertsFilePath = this.getTlsTrustCertsFilePath();

        if (tlsTrustCertsFilePath == null || tlsTrustCertsFilePath.isEmpty()) {
            return null;
        }

        final Path caCertificatePath = Paths.get(tlsTrustCertsFilePath);

        final CertificateFactory factory = CertificateFactory.getInstance("X.509");
        final Certificate trustedCa;
        try (final InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }

        final KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);

        final SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
        this.sslContext = sslContextBuilder.build();

        return this.sslContext;
    }
}
