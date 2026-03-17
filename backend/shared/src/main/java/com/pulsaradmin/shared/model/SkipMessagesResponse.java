package com.pulsaradmin.shared.model;

public record SkipMessagesResponse(
    String environmentId,
    String topicName,
    String subscriptionName,
    int skippedMessages,
    String message) {
}
