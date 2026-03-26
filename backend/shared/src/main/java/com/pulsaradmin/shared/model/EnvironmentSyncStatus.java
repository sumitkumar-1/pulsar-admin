package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.List;

public record EnvironmentSyncStatus(
    String environmentId,
    String syncStatus,
    String syncMessage,
    Instant lastSyncedAt,
    Instant syncStartedAt,
    Instant lastCompletedAt,
    int tenantCount,
    int namespaceCount,
    int topicCount,
    int warningCount,
    List<String> warnings) {
}
