/*
Copyright 2024 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.codenotary.immudblog4j;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.codenotary.immudblog4j.store.ImmudbStorageService;
import org.codenotary.immudblog4j.store.StorageService;
import org.codenotary.immudblog4j.store.VaultStorageService;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Plugin(name = "ImmudbAppender", category = "Core", elementType = Appender.ELEMENT_TYPE, printObject = true)
public class ImmudbAppender extends AbstractAppender {
    private static final String IMMUDB_STORAGE = "immudb";
    private static final String VAULT_STORAGE = "immudb-vault";

    private static final int MAX_PENDING_LOGS_DEFAULT = 100;
    private static final int MAX_PENDING_LOGS_BUFFER_SIZE_DEFAULT = 1024 * 1024; // 1MB
    private static final int SYNC_TIMEOUT_SECONDS_DEFAULT = 10;

    private final int maxPendingLogs;
    private final int maxPendingLogsBufferSize; // 1MB
    private final int syncTimeoutSeconds;

    private final StorageService storage;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);
    private final AtomicInteger bufferedDataSize = new AtomicInteger(0);
    private final AtomicLong lastSyncAt = new AtomicLong(0);
    private Queue<byte[]> outgoingMessages = new LinkedBlockingDeque<>();

    protected ImmudbAppender(String name, Layout<? extends Serializable> layout,
                             StorageService storage,
                             Integer maxPendingLogs,
                             Integer maxPendingLogsBufferSize,
                             Integer syncTimeoutSeconds
                                  ) throws MalformedURLException {
        super(name, null, layout, false);

        this.storage = storage;

        this.maxPendingLogs = maxPendingLogs != null ? maxPendingLogs : MAX_PENDING_LOGS_DEFAULT;
        this.maxPendingLogsBufferSize = maxPendingLogsBufferSize != null ? maxPendingLogsBufferSize : MAX_PENDING_LOGS_BUFFER_SIZE_DEFAULT;
        this.syncTimeoutSeconds = syncTimeoutSeconds != null ? syncTimeoutSeconds : SYNC_TIMEOUT_SECONDS_DEFAULT;
        this.lastSyncAt.set(Instant.now().getEpochSecond());
    }

    @Override
    public void append(LogEvent event) {
        byte[] message = getLayout().toByteArray(event);

        rwLock.readLock().lock();

        outgoingMessages.add(message);
        int bufSize = bufferedDataSize.addAndGet(message.length);

        rwLock.readLock().unlock();

        maybeStartSync(outgoingMessages.size(), bufSize);
    }

    private void maybeStartSync(int queueSize, int bufferSize) {
        if(!shouldStartSync(queueSize, bufferSize)) return;

        if(!syncInProgress.compareAndExchange(false, true)) {
            startSync();
        }
    }

    private void startSync() {
        Thread thread = new Thread(() -> {
            Queue<byte[]> messages = swapQueue();

            try {
                storage.store(messages);
            } catch (Exception ex) {
                LOGGER.error(ex);
            }

            lastSyncAt.set(Instant.now().getEpochSecond());
            syncInProgress.set(false);
        });
        thread.start();
    }

    private Queue<byte[]> swapQueue() {
        rwLock.writeLock().lock();

        Queue<byte[]> old = outgoingMessages;
        outgoingMessages = new LinkedBlockingDeque<>();
        bufferedDataSize.set(0);

        rwLock.writeLock().unlock();

        return old;
    }

    private boolean shouldStartSync(int queueSize, int bufferSize) {
        return queueSize >= maxPendingLogs ||
                bufferSize >= maxPendingLogsBufferSize ||
                Duration.between(Instant.ofEpochSecond(lastSyncAt.get()), Instant.now()).getSeconds() >= syncTimeoutSeconds;
    }

    @PluginFactory
    public static ImmudbAppender createAppender(
            @PluginBuilderAttribute("storage") String storage,
            @PluginBuilderAttribute("name") String name,

            // immudb storage parameters
            @PluginBuilderAttribute("host") String host,
            @PluginBuilderAttribute("port") Integer port,
            @PluginBuilderAttribute("username") String username,
            @PluginBuilderAttribute("password") String password,
            @PluginBuilderAttribute("database") String database,
            @PluginBuilderAttribute("table") String table,

            // vault storage parameters
            @PluginBuilderAttribute("writeToken") String writeToken,
            @PluginBuilderAttribute("ledger") String ledger,
            @PluginBuilderAttribute("collection") String collection,
            @PluginBuilderAttribute("maxPendingLogs") Integer maxPendingLogs,
            @PluginBuilderAttribute("maxPendingLogsBufferSize") Integer maxPendingLogsBufferSize,
            @PluginBuilderAttribute("syncTimeoutSeconds") Integer syncTimeoutSeconds) throws MalformedURLException {

        if(storage == null) {
            LOGGER.error("No storage type provided");
            return null;
        }

        StorageService storageService =
                switch (storage) {
                    case IMMUDB_STORAGE -> createImmudbStorage(host, port, username, password, database, table);
                    case VAULT_STORAGE -> createVaultStorage(writeToken, ledger, collection);
                    default -> null;
                };

        if(storageService == null) {
            LOGGER.error("unknown storage type {}", storage);
            return null;
        }

        if (name == null) {
            LOGGER.error("No name provided for ImmudbAppender");
            return null;
        }

        return new ImmudbAppender(
                name,
                JsonLayout.createDefaultLayout(),
                storageService,
                maxPendingLogs,
                maxPendingLogsBufferSize,
                syncTimeoutSeconds
        );
    }

    private static StorageService createVaultStorage(String writeToken, String ledger, String collection) {
        if (writeToken == null) {
            LOGGER.error("No writeToken provided for ImmudbAppender");
            return null;
        }
        return new VaultStorageService(writeToken, ledger, collection);
    }

    private static StorageService createImmudbStorage(String host, Integer port, String username, String password, String database, String table) {
        return new ImmudbStorageService(
                host,
                port,
                username,
                password,
                database,
                table
        );
    }
}

