package com.pulsaradmin.worker;

public record WorkerEnvironmentRecord(
    String id,
    String brokerUrl,
    String adminUrl,
    String authMode,
    String credentialReference,
    boolean tlsEnabled) {
}
