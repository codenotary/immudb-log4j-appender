# immudb-log4j-appender

This repository provides a custom [log4j2 Appender](https://logging.apache.org/log4j/2.x/manual/appenders.html) implementation for sending logs to [immudb](https://immudb.io) and [immudb-vault](https://vault.immudb.io) thus securing them with proof of no tampering.

Tamperproof log storage ensures data integrity, security, and compliance, while using a ledger database for log storage integrates these benefits with advanced features for querying, analysis, and performance. This combination creates a powerful solution for maintaining reliable, transparent, and secure log data easily using **log4j2**.

## Installation

To use `immudb-log4j-appender` inside your project, setup the jitpack repository and include the dependency:

### Maven

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.codenotary</groupId>
    <artifactId>immudb-log4j-appender</artifactId>
    <version>v0.0.1</version>
</dependency>
```

### Gradle

```kotlin
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenCentral()
        maven { url 'https://jitpack.io' }
    }
}

dependencies {
	implementation 'com.github.codenotary:immudb-log4j-appender:v0.0.1'
}
```

## Configuring the appender

You can configure the appender to either write **immudb** or **immudb vault** by setting up your `log42j.xml` config file as shown below:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Appenders>
        <Console name="ConsoleAppender" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"/>
        </Console>

        <ImmudbAppender
                name="ImmudbAppender"
                storage="immudb"
                host="localhost"
                port="3322"
                username="immudb"
                password="immudb"
                database="defaultdb"
                table="log4j_logs"
        />

        <!-- or, if you want to send logs to immudb vault -->
        <ImmudbAppender
                name="ImmudbAppender"
                storage="immudb-vault"
                writeToken="<your-immudb-vault-write-token>"
        />
    </Appenders>
    <Loggers>
        <Root level="debug">
            <AppenderRef ref="ImmudbAppender"/>
            <AppenderRef ref="ConsoleAppender"/>
        </Root>
    </Loggers>
</Configuration>
```

## Quick start

After correctly configuring your `log4j2.xml` config file, you are ready to go. Simply use **log4j** as usual:

```java
package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        ...
        logger.info("Hello, immudb!");
        ...
    }
}
```

## How it works

`immudb-log4j-appender` collects logs and sends them to the configured storage service in the background when at least one of the following conditions hold:

- the number of queued messages exceeds the value of the `maxPendingLogs` parameter (`100` by default);
- the total amount of queued data (in bytes) exceeds the value of the `maxPendingLogsBufferSize` parameter (`1MB` by default);
- at least `syncTimeoutSeconds` have passed since the last time the last logs were sent to the storage service (`10 seconds` by default).

It's important to mention that these conditions are evaluated only when a new log event is intercepted by the appender. As a result, the synchronization of queued data may be delayed during periods of inactivity.

Each of the parameter described above can be set by specifying a value for corresponding parameter in the `log4j2.xml` config:

```xml
<ImmudbAppender
    name="ImmudbAppender"
    storage="immudb-vault"
    writeToken="<your-immudb-vault-write-token>"
    maxPendingLogs="10"
    maxPendingLogsBufferSize="1024"
    syncTimeoutSeconds="20"
/>
```

## Contributing

We welcome contributions. Feel free to join the team!

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb-log4j-appender/issues).
