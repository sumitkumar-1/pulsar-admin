package com.pulsaradmin.shared.model;

public record TopicPolicies(
    Integer retentionTimeInMinutes,
    Integer retentionSizeInMb,
    Integer ttlInSeconds,
    Long compactionThresholdInBytes,
    Integer maxProducers,
    Integer maxConsumers,
    Integer maxSubscriptions) {
}
