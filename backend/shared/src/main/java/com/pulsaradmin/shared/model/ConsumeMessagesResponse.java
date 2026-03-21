package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.List;

public record ConsumeMessagesResponse(
    String environmentId,
    String topicName,
    String subscriptionName,
    boolean ephemeral,
    int requestedCount,
    int receivedCount,
    int waitTimeSeconds,
    boolean completed,
    Instant completedAt,
    String message,
    List<ConsumedMessage> messages,
    List<String> warnings) {
}
