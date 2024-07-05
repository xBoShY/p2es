package io.xboshy.pulsar.config;

import java.util.HashMap;
import java.util.Map;

public abstract class Config {
    final protected Map<String, Object> config;

    public Config(final Map<String, String> input) {
        this.config = new HashMap<>();
        for(final Map.Entry<String, String> entry : input.entrySet()) {
            if (!entry.getKey().startsWith(this.getPrefix()))
                continue;

            final String key = entry.getKey().substring(this.getPrefix().length());
            this.config.put(key, entry.getValue());
        }
    }

    public abstract String getPrefix();

    public Map<String, Object> getConfig() {
        return this.config;
    }

    public Object getObjValue(String property) {
        return config.get(property);
    }

    public String getStrValue(String property) {
        return (String) this.getObjValue(property);
    }

    public String getStrValue(String property, String def) {
        String val = this.getStrValue(property);
        if (val == null)
            return def;

        return val;
    }

    public Integer getIntValue(String property) {
        String str = this.getStrValue(property);
        if (str == null)
            return null;

        return Integer.parseInt(str);
    }

    public Integer getIntValue(String property, Integer def) {
        Integer val = this.getIntValue(property);
        if (val == null)
            return def;

        return val;
    }

    public Boolean getBoolValue(String property) {
        String str = this.getStrValue(property);
        if (str == null)
            return null;

        return Boolean.valueOf(str);
    }

    public Boolean getBoolValue(String property, Boolean def) {
        String str = this.getStrValue(property);
        if (str == null)
            return def;

        return Boolean.valueOf(str);
    }
}
