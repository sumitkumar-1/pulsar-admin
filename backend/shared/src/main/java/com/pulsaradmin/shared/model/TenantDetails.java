package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.List;

public record TenantDetails(
    String environmentId,
    String tenant,
    List<String> adminRoles,
    List<String> allowedClusters,
    int namespaceCount,
    int topicCount,
    Instant lastSyncedAt) {
}
