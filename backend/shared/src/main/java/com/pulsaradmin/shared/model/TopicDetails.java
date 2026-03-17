package com.pulsaradmin.shared.model;

import java.util.List;

public record TopicDetails(
    String fullName,
    String tenant,
    String namespace,
    String topic,
    boolean partitioned,
    int partitions,
    TopicHealth health,
    TopicStatsSummary stats,
    SchemaSummary schema,
    String ownerTeam,
    String notes,
    List<TopicPartitionSummary> partitionSummaries,
    List<String> subscriptions) {
}
