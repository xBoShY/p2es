package io.xboshy.pulsar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class CustomThreadFactory implements ThreadFactory {
    private static final Logger logger = LogManager.getLogger(CustomThreadFactory.class);
    private final String name;
    private final AtomicInteger id = new AtomicInteger(0);


    public CustomThreadFactory(final String name) {
        this.name = name == null ? "unamed-thread" : name;
    }

    public Thread newThread(@Nullable final Runnable command) {
        final String name = this.name + ": " + id.incrementAndGet();
        String logmsg = String.format("\"step\": \"spawnthread\" , \"threadname\": %s", name);

        final Thread thread = new Thread(command);
        thread.setName(name);
        CustomThreadFactory.logger.info(logmsg);
        thread.setDaemon(true);
        return thread;
    }
}
