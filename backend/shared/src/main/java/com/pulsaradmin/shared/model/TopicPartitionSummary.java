package com.pulsaradmin.shared.model;

public record TopicPartitionSummary(
    String partitionName,
    long backlog,
    int consumers,
    double publishRateIn,
    double dispatchRateOut,
    TopicHealth health) {
}
