package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.List;

public record ExportMessagesResponse(
    String environmentId,
    String topicName,
    String source,
    int exportedCount,
    String fileName,
    String contentType,
    String content,
    Instant exportedAt,
    String message,
    List<String> warnings) {
}
