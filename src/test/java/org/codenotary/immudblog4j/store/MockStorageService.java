package org.codenotary.immudblog4j.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class MockStorageService implements StorageService {
    private final List<byte[]> messages = new ArrayList<>();
    private final AtomicInteger storeCalls = new AtomicInteger();

    @Override
    public void store(Queue<byte[]> msgQueue) {
        synchronized (messages) {
            messages.addAll(msgQueue);
        }
        storeCalls.addAndGet(1);
    }

    public int size() {
        return messages.size();
    }

    public int calls() {
        return storeCalls.get();
    }

    public List<byte[]> getMessages() {
        return messages;
    }
}
