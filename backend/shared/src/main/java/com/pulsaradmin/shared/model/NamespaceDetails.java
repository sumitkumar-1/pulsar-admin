package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.List;

public record NamespaceDetails(
    String environmentId,
    String tenant,
    String namespace,
    int topicCount,
    List<TopicListItem> topics,
    NamespacePolicies policies,
    Instant lastSyncedAt,
    String syncMessage) {
}
