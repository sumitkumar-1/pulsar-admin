package com.pulsaradmin.shared.model;

import java.time.Instant;

public record EnvironmentDetails(
    String id,
    String name,
    String kind,
    EnvironmentStatus status,
    String region,
    String clusterLabel,
    String summary,
    String brokerUrl,
    String adminUrl,
    String authMode,
    String credentialReference,
    String syncTargets,
    boolean tlsEnabled,
    String syncStatus,
    String syncMessage,
    Instant lastSyncedAt,
    String lastTestStatus,
    String lastTestMessage,
    Instant lastTestedAt,
    boolean deleted) {
}
