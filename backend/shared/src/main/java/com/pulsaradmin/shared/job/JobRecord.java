package com.pulsaradmin.shared.job;

import java.time.Instant;
import java.util.Map;

public record JobRecord(
    String id,
    JobType type,
    String environmentId,
    JobStatus status,
    Map<String, Object> parameters,
    Instant createdAt,
    Instant updatedAt) {
}
