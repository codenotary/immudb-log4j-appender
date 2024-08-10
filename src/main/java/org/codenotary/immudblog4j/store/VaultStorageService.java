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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Queue;

public class VaultStorageService implements StorageService {
    private static final String URL_PATTERN = "https://vault.immudb.io/ics/api/v1/ledger/%s/collection/%s/document";

    private final String writeToken;
    private final URL vaultURL;

    public VaultStorageService(String writeToken, String ledger, String collection) {
        this.writeToken = writeToken;
        try {
            this.vaultURL = URI.create(String.format(URL_PATTERN, ledger != null ? ledger : "default", collection != null ? collection : "default")).toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void store(Queue<byte[]> msgQueue) throws IOException {
        for (byte[] msg : msgQueue) {
            storeMsg(msg);
        }
    }

    private void storeMsg(byte[] msg) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) vaultURL.openConnection();

        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Content-Type", "application/json; utf-8");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("X-API-Key", writeToken);
        connection.setDoOutput(true);

        try (OutputStream os = connection.getOutputStream()) {
            os.write(msg);
        }

        int code = connection.getResponseCode();
        String message = connection.getResponseMessage();

        if (code != HttpURLConnection.HTTP_OK)
            throw new IOException(String.format("unable to upload log to vault: code=%d, message=%s", code, message));

        connection.disconnect();
    }
}
