package com.pulsaradmin.shared.model;

import java.time.Instant;

public record EnvironmentSummary(
    String id,
    String name,
    String kind,
    EnvironmentStatus status,
    String region,
    String clusterLabel,
    String summary,
    String syncStatus,
    Instant lastSyncedAt,
    String lastTestStatus) {
}
