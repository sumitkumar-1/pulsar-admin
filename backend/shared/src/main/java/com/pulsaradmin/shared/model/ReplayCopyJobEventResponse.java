package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.Map;

public record ReplayCopyJobEventResponse(
    long id,
    String jobId,
    String eventType,
    Map<String, Object> details,
    Instant createdAt) {
}
