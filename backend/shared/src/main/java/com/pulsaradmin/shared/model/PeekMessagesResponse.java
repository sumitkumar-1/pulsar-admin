package com.pulsaradmin.shared.model;

import java.util.List;

public record PeekMessagesResponse(
    String environmentId,
    String topicName,
    int requestedCount,
    int returnedCount,
    boolean truncated,
    int scannedTopicCount,
    String message,
    List<PeekMessage> messages) {
}
