package io.xboshy.pulsar.config;

import java.util.Map;

public class PulsarClientConfig extends Config {
    final private String clusterNameString;
    public PulsarClientConfig(final Map<String, String> config) {
        super(config);
        this.clusterNameString = this.getStrValue("clusterName");
        this.config.remove("clusterName");
    }

    @Override
    public String getPrefix() {
        return "PULSAR_CLIENT_";
    }

    public String getClusterName() {
        return this.clusterNameString;
    }
}
