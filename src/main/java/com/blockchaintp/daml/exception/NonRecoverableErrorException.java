package com.blockchaintp.daml.exception;

/**
 * A non-recoverable exception is an unchecked exception that typically wraps a
 * service-level exception (the cause).
 */
public class NonRecoverableErrorException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NonRecoverableErrorException() {
        super();
    }

    public NonRecoverableErrorException(String message) {
        super(message);
    }

    public NonRecoverableErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonRecoverableErrorException(Throwable cause) {
        super(cause);
    }

    protected NonRecoverableErrorException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}