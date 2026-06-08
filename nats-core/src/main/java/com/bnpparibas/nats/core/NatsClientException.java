package com.bnpparibas.nats.core;

public class NatsClientException extends RuntimeException {
    public NatsClientException(String message) {
        super(message);
    }

    public NatsClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
