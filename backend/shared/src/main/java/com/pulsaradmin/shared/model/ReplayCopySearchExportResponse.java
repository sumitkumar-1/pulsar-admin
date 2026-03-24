package com.pulsaradmin.shared.model;

import java.time.Instant;

public record ReplayCopySearchExportResponse(
    String environmentId,
    String jobId,
    String topicName,
    int exportedCount,
    String fileName,
    String contentType,
    String content,
    Instant exportedAt,
    String message) {
}
