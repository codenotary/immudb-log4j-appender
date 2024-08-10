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

package org.codenotary.immudblog4j.store;

import io.codenotary.immudb4j.ImmuClient;
import io.codenotary.immudb4j.sql.SQLException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class ImmudbStorageService implements StorageService {
    private static final int BATCH_SIZE = 100;
    private static final String DEFAULT_TABLE_NAME = "log4j_logs";

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String database;
    private final String table;

    private boolean tableExists = false;
    private ImmuClient client;

    public ImmudbStorageService(
            String host,
            Integer port,
            String username,
            String password,
            String database,
            String table
    ) {
        this.host = host != null && !host.isEmpty() ? host : "localhost";
        this.port = port != null ? port : 3322;
        this.username = username != null ? username : "immudb";
        this.password = password != null ? password : "immudb";
        this.database = database != null ? database : "defaultdb";
        this.table = table == null ? DEFAULT_TABLE_NAME : table;
    }

    private ImmuClient getClient() {
        if(client == null) {
            ImmuClient cli = ImmuClient.newBuilder().
                    withServerUrl(this.host)
                    .withServerPort(this.port)
                    .build();

            cli.openSession(database, username, password);
            client = cli;
        }
        return client;
    }

    @Override
    public void store(Queue<byte[]> msgQueue) throws IOException {
        ImmuClient cli = getClient();

        createTableIfNotExists(cli);

        List<byte[]> allMessages = new ArrayList<>(msgQueue);
        for(int i = 0; i < allMessages.size(); i += BATCH_SIZE) {
            List<byte[]> batch = allMessages.subList(i, Math.min(allMessages.size(), i+BATCH_SIZE));
            sendBatch(cli, batch);
        }

    }

    private void sendBatch(ImmuClient cli, List<byte[]> msgBatch) throws IOException {
        String tuples = msgBatch.stream().map(msg -> "('" + new String(msg) + "')")
                .collect(Collectors.joining(","));

        exec(cli::beginTransaction, false);

        try {
            exec(() -> cli.sqlExec(String.format("INSERT INTO %s(data) VALUES %s", table, tuples)), false);
            exec(cli::commitTransaction, false);
        } finally {
            exec(cli::rollbackTransaction, true);
        }
    }

    private void createTableIfNotExists(ImmuClient cli) throws IOException {
        if(tableExists) return;

        exec(cli::beginTransaction, false);

        try {
            exec(() -> cli.sqlExec(
                    String.format(
                            "CREATE TABLE IF NOT EXISTS %s(id INTEGER AUTO_INCREMENT, data JSON, PRIMARY KEY id);",
                            table
                    )
            ), false);
            exec(cli::commitTransaction, false);
            tableExists = true;
        } finally {
            exec(cli::rollbackTransaction, true);
        }
    }

    interface SQLStatement {
        void exec() throws SQLException;
    }

    private void exec(SQLStatement stmt, boolean ignoreEx) throws IOException {
        try {
          stmt.exec();
        } catch (Exception ex) {
            if(!ignoreEx)
                throw new IOException(ex);
        }
    }
}
