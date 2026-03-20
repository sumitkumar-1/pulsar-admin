package com.pulsaradmin.shared.model;

public record NamespacePolicies(
    Integer retentionTimeInMinutes,
    Integer retentionSizeInMb,
    Integer messageTtlInSeconds,
    Boolean deduplicationEnabled,
    Long backlogQuotaLimitInBytes,
    Integer backlogQuotaLimitTimeInSeconds,
    Integer dispatchRatePerTopicInMsg,
    Long dispatchRatePerTopicInByte,
    Integer publishRateInMsg,
    Long publishRateInByte) {
}
