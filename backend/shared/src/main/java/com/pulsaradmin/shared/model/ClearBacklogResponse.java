package com.pulsaradmin.shared.model;

import java.time.Instant;

public record ClearBacklogResponse(
    String environmentId,
    String topicName,
    String subscriptionName,
    boolean cleared,
    String message,
    Instant clearedAt) {
}
