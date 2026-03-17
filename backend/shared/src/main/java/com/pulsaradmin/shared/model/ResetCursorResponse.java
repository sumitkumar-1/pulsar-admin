package com.pulsaradmin.shared.model;

public record ResetCursorResponse(
    String environmentId,
    String topicName,
    String subscriptionName,
    String target,
    String effectiveTimestamp,
    String message) {
}
