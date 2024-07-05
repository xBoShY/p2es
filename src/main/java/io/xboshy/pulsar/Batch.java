package io.xboshy.pulsar;

import org.apache.pulsar.client.api.Messages;

public class Batch<T> {
    private Messages<T> msgs;

    public Messages<T> getMessages() {
        return msgs;
    }

    public void setMessages(final Messages<T> msgs) {
        this.msgs = msgs;
    }

    public void clear() {
        this.msgs = null;
    }
}
