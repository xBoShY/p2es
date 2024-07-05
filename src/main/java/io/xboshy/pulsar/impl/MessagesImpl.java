package io.xboshy.pulsar.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

public class MessagesImpl<T> implements Messages<T> {
    final private List<Message<T>> messageList;
    private final int maxNumberOfMessages;
    private final long maxSizeOfMessages;
    private long currentSizeOfMessages;
    private long firstMillis;

    public MessagesImpl(final int maxNumberOfMessages, final long maxSizeOfMessages) {
        this.maxNumberOfMessages = maxNumberOfMessages;
        this.maxSizeOfMessages = maxSizeOfMessages;
        this.messageList = maxNumberOfMessages > 0 ? new ArrayList<>(maxNumberOfMessages) : new ArrayList<>();
    }

    public boolean canAdd(final Message<T> message) {
        if (this.size() == 0) {
            return true;
        } else if (this.maxNumberOfMessages > 0 && this.size() + 1 > this.maxNumberOfMessages) {
            return false;
        } else {
            return this.maxSizeOfMessages <= 0L || this.currentSizeOfMessages + (long)message.size() <= this.maxSizeOfMessages;
        }
    }

    public void add(final Message<T> message) throws Exception {
        if (message != null) {
            if (!this.canAdd(message)) {
                throw new Exception("No more space to add messages");
            }
            if (this.size() == 0) {
                firstMillis = System.currentTimeMillis();
            }
            this.currentSizeOfMessages += message.size();
            this.messageList.add(message);
        }
    }

    public int size() {
        return this.messageList.size();
    }

    public long age() {
        return System.currentTimeMillis() - firstMillis;
    }

    public void clear() {
        this.currentSizeOfMessages = 0L;
        this.messageList.clear();
    }

    public Iterator<Message<T>> iterator() {
        return this.messageList.iterator();
    }
}
