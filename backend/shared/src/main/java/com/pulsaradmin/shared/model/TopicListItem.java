package com.pulsaradmin.shared.model;

public record TopicListItem(
    String fullName,
    String tenant,
    String namespace,
    String topic,
    boolean partitioned,
    int partitions,
    boolean schemaPresent,
    TopicHealth health,
    TopicStatsSummary stats,
    String summary) {
}
