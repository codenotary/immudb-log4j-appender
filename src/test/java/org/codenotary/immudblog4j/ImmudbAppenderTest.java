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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.JsonLayout;
import org.codenotary.immudblog4j.store.MockStorageService;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;

public class ImmudbAppenderTest {
    private MockStorageService storage;

    private Logger setup(Integer maxPendingLogs, Integer maxPendingLogsBufferSize, Integer syncTimeoutSeconds) throws MalformedURLException {
        long now = System.currentTimeMillis();

        Logger logger = LogManager.getLogger("TestLogger_" + now);

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();

        storage = new MockStorageService();

        Appender appender = new ImmudbAppender(
                "ImmudbAppender_" + now,
                JsonLayout.createDefaultLayout(),
                storage,
                maxPendingLogs,
                maxPendingLogsBufferSize,
                syncTimeoutSeconds
        );

        appender.start();
        config.addAppender(appender);

        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        loggerConfig.setLevel(Level.INFO);
        loggerConfig.addAppender(appender, Level.INFO, null);
        context.updateLoggers();

        return logger;
    }

    @Test
    public void testAppenderTriggeredOnMaxPendingLogBufferSize() throws InterruptedException, MalformedURLException {
        Logger logger = setup(10, 1, null);

        for(int i = 0; i<100; i++) {
            logger.info("test message {}", i);
        }

        Thread.sleep(1000);

        checkMessages(100, 100);
    }

    @Test
    public void testAppenderTriggeredOnMaxPendingLogs() throws InterruptedException, MalformedURLException {
        Logger logger = setup(10, 1024 * 1024 * 1024, null);

        for(int i = 0; i<109; i++) {
            logger.info("test message {}", i);
        }

        Thread.sleep(1000);

        checkMessages(100, 10);
    }

    @Test
    public void testAppenderTriggeredOnSyncTimeout() throws InterruptedException, MalformedURLException {
        Logger logger = setup(10000, 1024 * 1024 * 1024, 5);

        for(int i = 0; i<98; i++) {
            logger.info("test message {}", i);
        }

        checkMessages(0, 0);

        Thread.sleep(2000);

        logger.info("test message {}", 98);

        Thread.sleep(1000);

        checkMessages(0, 0);

        Thread.sleep(2000);

        logger.info("test message {}", 99);

        Thread.sleep(500);

        checkMessages(100, 1);
    }

    private void checkMessages(int n, int calls) {
        Assert.assertEquals(n, storage.size());
        Assert.assertEquals(calls, storage.calls());

        int i = 0;
        for(byte[] msg : storage.getMessages()) {
            Assert.assertTrue(new String(msg).contains(String.format("test message %d", i++)));
        }
    }
}