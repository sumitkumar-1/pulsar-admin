package com.pulsaradmin.shared.model;

import java.time.Instant;

public record EnvironmentSyncStatus(
    String environmentId,
    String syncStatus,
    String syncMessage,
    Instant lastSyncedAt,
    int tenantCount,
    int namespaceCount,
    int topicCount) {
}
