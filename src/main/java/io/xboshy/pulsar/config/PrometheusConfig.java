package io.xboshy.pulsar.config;

import java.util.Map;

public class PrometheusConfig extends Config {
    private Integer port = null;
    public PrometheusConfig(final Map<String, String> config) {
        super(config);
    }

    @Override
    public String getPrefix() {
        return "PROMETHEUS_";
    }

    public int getPort() {
        if (this.port != null)
            return this.port;

        this.port = this.getIntValue("port", 8081);

        return this.port;
    }
}
