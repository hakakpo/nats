package com.bnpparibas.nats.core;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultNatsConnectionManager implements NatsConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNatsConnectionManager.class);

    private final Options options;
    private final ExecutorService executor;
    private final boolean reconnectOnConnect;
    private final AtomicReference<CompletableFuture<Connection>> connection = new AtomicReference<>();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public DefaultNatsConnectionManager(Options options, ExecutorService executor, boolean reconnectOnConnect) {
        this.options = options;
        this.executor = executor;
        this.reconnectOnConnect = reconnectOnConnect;
    }

    @Override
    public CompletionStage<Connection> connectAsync() {
        CompletableFuture<Connection> existing = connection.get();
        if (existing != null && !existing.isCompletedExceptionally()) {
            return existing;
        }

        CompletableFuture<Connection> started = CompletableFuture.supplyAsync(() -> {
            if (closing.get()) {
                throw new NatsClientException("NATS connection manager is closing");
            }
            try {
                LOGGER.info("Connecting to NATS servers {}", options.getServers());
                return reconnectOnConnect ? Nats.connectReconnectOnConnect(options) : Nats.connect(options);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new NatsClientException("Interrupted while connecting to NATS", ex);
            } catch (Exception ex) {
                throw new NatsClientException("Unable to connect to NATS", ex);
            }
        }, executor);

        if (connection.compareAndSet(existing, started)) {
            return started;
        }
        return connection.get();
    }

    @Override
    public CompletionStage<Connection> connection() {
        return connectAsync();
    }

    @Override
    public Optional<Connection> currentConnection() {
        CompletableFuture<Connection> future = connection.get();
        if (future == null || !future.isDone() || future.isCompletedExceptionally()) {
            return Optional.empty();
        }
        return Optional.ofNullable(future.getNow(null));
    }

    @Override
    public Connection.Status status() {
        return currentConnection().map(Connection::getStatus).orElse(Connection.Status.DISCONNECTED);
    }

    @Override
    public CompletionStage<Void> drainAsync() {
        return CompletableFuture.runAsync(() -> currentConnection().ifPresent(conn -> {
            try {
                conn.drain(options.getConnectionTimeout()).get(options.getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException ex) {
                throw new NatsClientException("Timed out while draining NATS connection", ex);
            } catch (Exception ex) {
                throw new NatsClientException("Unable to drain NATS connection", ex);
            }
        }), executor);
    }

    @Override
    public CompletionStage<Void> closeAsync() {
        closing.set(true);
        return CompletableFuture.runAsync(() -> currentConnection().ifPresent(conn -> {
            try {
                conn.close();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new NatsClientException("Interrupted while closing NATS connection", ex);
            }
        }), executor);
    }
}
