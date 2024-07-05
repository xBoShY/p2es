package io.xboshy.pulsar.config;

import java.util.Map;

public class GlobalConfig extends Config {
    public enum IdModes {
        NONE,
        MSGID,
        HASH,
        KEY,
        KEYHASH
    }
    final private Integer inflightBatches;
    final private Integer ringBuffer;
    final private IdModes idMode;
    public GlobalConfig(Map<String, String> config) throws Exception {
        super(config);
        this.inflightBatches = this.getIntValue("inflightBatches", 1);
        this.ringBuffer = this.getIntValue("ringBuffer", 2);

        String idModeStr = this.getStrValue("idMode", "none").toUpperCase();
        this.idMode = IdModes.valueOf(idModeStr);

        if (!isPowerOfTwo(this.ringBuffer)) {
            throw new Exception(this.getPrefix() + "ringBuffer must be a power of 2");
        }
    }

    private boolean isPowerOfTwo(int n) {
        return n != 0 && ((n & (n - 1)) == 0);
    }

    @Override
    public String getPrefix() {
        return "GLOBAL_";
    }

    public int getInflightBatches() {
        return this.inflightBatches;
    }

    public int getRingBuffer() {
        return this.ringBuffer;
    }

    public IdModes getIdMode() {
        return this.idMode;
    }
}
