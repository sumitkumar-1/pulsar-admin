package com.pulsaradmin.shared.model;

import java.time.Instant;

public record EnvironmentConnectionTestResult(
    String environmentId,
    boolean successful,
    String status,
    String message,
    Instant testedAt,
    boolean syncTriggered) {
}
