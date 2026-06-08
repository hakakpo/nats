package com.bnpparibas.nats.core;

import java.util.List;

public record NatsValidationReport(boolean valid, List<String> errors, List<String> warnings) {
    public NatsValidationReport {
        errors = errors == null ? List.of() : List.copyOf(errors);
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }

    public static NatsValidationReport emptyValidReport() {
        return new NatsValidationReport(true, List.of(), List.of());
    }
}
